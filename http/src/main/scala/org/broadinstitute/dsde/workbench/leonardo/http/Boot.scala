package org.broadinstitute.dsde.workbench.leonardo
package http

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.Uri.Host
import cats.effect._
import cats.effect.std.{Dispatcher, Queue, Semaphore}
import cats.mtl.Ask
import cats.syntax.all._
import cats.{Monad, Parallel}
import com.github.benmanes.caffeine.cache.Caffeine
import com.google.api.services.compute.ComputeScopes
import com.google.api.services.container.ContainerScopes
import com.google.auth.oauth2.GoogleCredentials
import fs2.Stream
import io.circe.syntax._
import io.kubernetes.client.openapi.ApiClient
import org.broadinstitute.dsde.workbench.google.GoogleCredentialModes.Json
import org.broadinstitute.dsde.workbench.google.{HttpGoogleDirectoryDAO, HttpGoogleIamDAO}
import org.broadinstitute.dsde.workbench.google2.GKEModels.KubernetesClusterId
import org.broadinstitute.dsde.workbench.google2.util.RetryPredicates
import org.broadinstitute.dsde.workbench.google2.{
  credentialResource,
  ComputePollOperation,
  Event,
  GKEService,
  GoogleComputeService,
  GoogleDataprocService,
  GoogleDiskService,
  GooglePublisher,
  GoogleResourceService,
  GoogleStorageService,
  GoogleSubscriber
}
import org.broadinstitute.dsde.workbench.leonardo.AsyncTaskProcessor.Task
import org.broadinstitute.dsde.workbench.leonardo.auth.{AuthCacheKey, PetClusterServiceAccountProvider, SamAuthProvider}
import org.broadinstitute.dsde.workbench.leonardo.config.Config._
import org.broadinstitute.dsde.workbench.leonardo.config.LeoExecutionModeConfig
import org.broadinstitute.dsde.workbench.leonardo.dao._
import org.broadinstitute.dsde.workbench.leonardo.dao.google.GoogleOAuth2Service
import org.broadinstitute.dsde.workbench.leonardo.db.DbReference
import org.broadinstitute.dsde.workbench.leonardo.dns._
import org.broadinstitute.dsde.workbench.leonardo.http.api.{BuildTimeVersion, HttpRoutes, StandardUserInfoDirectives}
import org.broadinstitute.dsde.workbench.leonardo.http.service._
import org.broadinstitute.dsde.workbench.leonardo.model.ServiceAccountProvider
import org.broadinstitute.dsde.workbench.leonardo.monitor.LeoPubsubCodec.leoPubsubMessageDecoder
import org.broadinstitute.dsde.workbench.leonardo.monitor.NonLeoMessageSubscriber.nonLeoMessageDecoder
import org.broadinstitute.dsde.workbench.leonardo.monitor._
import org.broadinstitute.dsde.workbench.leonardo.util._
import org.broadinstitute.dsde.workbench.model.{IP, TraceId, UserInfo}
import org.broadinstitute.dsde.workbench.openTelemetry.OpenTelemetryMetrics
import org.broadinstitute.dsp.HelmInterpreter
import org.http4s.blaze.client
import org.http4s.client.middleware.{Retry, RetryPolicy, Logger => Http4sLogger}
import org.typelevel.log4cats.StructuredLogger
import org.typelevel.log4cats.slf4j.Slf4jLogger
import scalacache.caffeine._

import java.nio.file.Paths
import java.time.Instant
import java.util.concurrent.TimeUnit
import javax.net.ssl.SSLContext
import scala.concurrent.ExecutionContext
import scala.concurrent.duration._
import scala.jdk.CollectionConverters._

object Boot extends IOApp {
  val workbenchMetricsBaseName = "google"

  private def startup(): IO[Unit] = {
    // We need an ActorSystem to host our application in
    implicit val system = ActorSystem(applicationConfig.applicationName)
    import system.dispatcher
    implicit val logger =
      StructuredLogger.withContext[IO](Slf4jLogger.getLogger[IO])(
        Map(
          "serviceContext" -> org.broadinstitute.dsde.workbench.leonardo.http.serviceData.asJson.toString,
          "version" -> BuildTimeVersion.version.getOrElse("unknown")
        )
      )

    createDependencies[IO](applicationConfig.leoServiceAccountJsonFile.toString).use { appDependencies =>
      val googleDependencies = appDependencies.googleDependencies

      implicit val dbRef = appDependencies.dbReference
      implicit val openTelemetry = googleDependencies.openTelemetryMetrics

      val dateAccessedUpdater =
        new DateAccessedUpdater(dateAccessUpdaterConfig, appDependencies.dateAccessedUpdaterQueue)
      val proxyService = new ProxyService(
        appDependencies.sslContext,
        proxyConfig,
        appDependencies.jupyterDAO,
        appDependencies.runtimeDnsCache,
        googleDependencies.kubernetesDnsCache,
        appDependencies.authProvider,
        appDependencies.dateAccessedUpdaterQueue,
        googleDependencies.googleOauth2DAO,
        appDependencies.proxyResolver,
        appDependencies.samDAO,
        appDependencies.googleTokenCache,
        appDependencies.samResourceCache
      )
      val statusService = new StatusService(appDependencies.samDAO, appDependencies.dbReference)
      val runtimeServiceConfig = RuntimeServiceConfig(
        proxyConfig.proxyUrlBase,
        imageConfig,
        autoFreezeConfig,
        dataprocConfig,
        gceConfig,
        AzureServiceConfig(
          //For now azure disks share same defaults as normal disks
          ConfigReader.appConfig.persistentDisk,
          ConfigReader.appConfig.azure.service
        ),
        ConfigReader.appConfig.azure.runtimeDefaults
      )
      val runtimeService = RuntimeService(
        runtimeServiceConfig,
        ConfigReader.appConfig.persistentDisk,
        appDependencies.authProvider,
        appDependencies.serviceAccountProvider,
        appDependencies.dockerDAO,
        googleDependencies.googleStorageService,
        googleDependencies.googleComputeService,
        appDependencies.publisherQueue
      )
      val diskService = new DiskServiceInterp[IO](
        ConfigReader.appConfig.persistentDisk,
        appDependencies.authProvider,
        appDependencies.serviceAccountProvider,
        appDependencies.publisherQueue
      )

      val leoKubernetesService: LeoAppServiceInterp[IO] =
        new LeoAppServiceInterp(appDependencies.authProvider,
                                appDependencies.serviceAccountProvider,
                                leoKubernetesConfig,
                                appDependencies.publisherQueue,
                                appDependencies.googleDependencies.googleComputeService)

      val azureService = new AzureServiceInterp[IO](
        runtimeServiceConfig,
        appDependencies.authProvider,
        appDependencies.wsmDAO,
        appDependencies.samDAO,
        appDependencies.asyncTasksQueue,
        appDependencies.publisherQueue
      )

      val httpRoutes = new HttpRoutes(
        swaggerConfig,
        statusService,
        proxyService,
        runtimeService,
        diskService,
        leoKubernetesService,
        azureService,
        StandardUserInfoDirectives,
        contentSecurityPolicy,
        refererConfig
      )
      val httpServer = for {
        start <- IO.realTimeInstant
        implicit0(ctx: Ask[IO, AppContext]) = Ask.const[IO, AppContext](
          AppContext(TraceId(s"Boot_${start}"), start)
        )
//       TODO: this will be needed once we support more environments.
//        Disable for now since this causes fiab start to fail
//        _ <- appDependencies.samDAO.registerLeo

        _ <- if (leoExecutionModeConfig == LeoExecutionModeConfig.BackLeoOnly) {
          appDependencies.dataprocInterp.setupDataprocImageGoogleGroup
        } else IO.unit
        _ <- IO.fromFuture {
          IO {
            Http()
              .newServerAt("0.0.0.0", 8080)
              .bindFlow(httpRoutes.route)
              .onError {
                case t: Throwable =>
                  logger
                    .error(t)("FATAL - failure starting http server")
                    .unsafeToFuture()(cats.effect.unsafe.IORuntime.global)
              }
          }
        }
      } yield ()

      val allStreams = {
        val asyncTasks = AsyncTaskProcessor(asyncTaskProcessorConfig, appDependencies.asyncTasksQueue)

        val backLeoOnlyProcesses = {
          val monitorAtBoot =
            new MonitorAtBoot[IO](appDependencies.publisherQueue, googleDependencies.googleComputeService)

          val autopauseMonitor = AutopauseMonitor(
            autoFreezeConfig,
            appDependencies.jupyterDAO,
            appDependencies.publisherQueue
          )

          val nonLeoMessageSubscriber =
            new NonLeoMessageSubscriber[IO](appDependencies.gkeAlg,
                                            googleDependencies.googleComputeService,
                                            appDependencies.samDAO,
                                            appDependencies.nonLeoMessageGoogleSubscriber,
                                            googleDependencies.cryptoMiningUserPublisher,
                                            appDependencies.asyncTasksQueue)

          List(
            nonLeoMessageSubscriber.process,
            Stream.eval(appDependencies.nonLeoMessageGoogleSubscriber.start),
            asyncTasks.process,
            appDependencies.pubsubSubscriber.process,
            Stream.eval(appDependencies.subscriber.start),
            monitorAtBoot.process, // checks database to see if there's on-going runtime status transition
            autopauseMonitor.process // check database to autopause runtimes periodically
          )
        }

        val uniquefrontLeoOnlyProcesses = List(
          dateAccessedUpdater.process // We only need to update dateAccessed in front leo
        ) ++ appDependencies.recordCacheMetrics

        val extraProcesses = leoExecutionModeConfig match {
          case LeoExecutionModeConfig.BackLeoOnly  => backLeoOnlyProcesses
          case LeoExecutionModeConfig.FrontLeoOnly => asyncTasks.process :: uniquefrontLeoOnlyProcesses
          case LeoExecutionModeConfig.Combined     => backLeoOnlyProcesses ++ uniquefrontLeoOnlyProcesses
        }

        List(
          appDependencies.leoPublisher.process, //start the publisher queue .dequeue
          Stream.eval[IO, Unit](httpServer) //start http server
        ) ++ extraProcesses
      }

      val app = Stream.emits(allStreams).covary[IO].parJoin(allStreams.length)

      app
        .handleErrorWith(error => Stream.eval(logger.error(error)("Failed to start leonardo")))
        .compile
        .drain
    }
  }

  private def createDependencies[F[_]: Parallel](
    pathToCredentialJson: String
  )(implicit logger: StructuredLogger[F],
    ec: ExecutionContext,
    as: ActorSystem,
    F: Async[F]): Resource[F, AppDependencies[F]] =
    for {
      semaphore <- Resource.eval(Semaphore[F](applicationConfig.concurrency))
      // This is for sending custom metrics to stackdriver. all custom metrics starts with `OpenCensus/leonardo/`.
      // Typing in `leonardo` in metrics explorer will show all leonardo custom metrics.
      // As best practice, we should have all related metrics under same prefix separated by `/`
      implicit0(openTelemetry: OpenTelemetryMetrics[F]) <- OpenTelemetryMetrics
        .resource[F](applicationConfig.applicationName, prometheusConfig.endpointPort)

      // Set up database reference
      concurrentDbAccessPermits <- Resource.eval(Semaphore[F](dbConcurrency))
      implicit0(dbRef: DbReference[F]) <- DbReference.init(liquibaseConfig, concurrentDbAccessPermits)

      // Set up DNS caches
      hostToIpMapping <- Resource.eval(Ref.of(Map.empty[Host, IP]))
      proxyResolver <- Dispatcher[F].map(d => ProxyResolver(hostToIpMapping, d))

      underlyingRuntimeDnsCache = buildCache[RuntimeDnsCacheKey, scalacache.Entry[HostStatus]](
        runtimeDnsCacheConfig.cacheMaxSize,
        runtimeDnsCacheConfig.cacheExpiryTime
      )
      runtimeDnsCaffineCache <- Resource.make(
        F.delay(CaffeineCache[F, RuntimeDnsCacheKey, HostStatus](underlyingRuntimeDnsCache))
      )(s => F.delay(s.close))
      runtimeDnsCache = new RuntimeDnsCache(proxyConfig, dbRef, hostToIpMapping, runtimeDnsCaffineCache)
      underlyingKubernetesDnsCache = buildCache[KubernetesDnsCacheKey, scalacache.Entry[HostStatus]](
        kubernetesDnsCacheConfig.cacheMaxSize,
        kubernetesDnsCacheConfig.cacheExpiryTime
      )

      kubernetesDnsCaffineCache <- Resource.make(
        F.delay(CaffeineCache[F, KubernetesDnsCacheKey, HostStatus](underlyingKubernetesDnsCache))
      )(s => F.delay(s.close))
      kubernetesDnsCache = new KubernetesDnsCache(proxyConfig, dbRef, hostToIpMapping, kubernetesDnsCaffineCache)

      // Set up SSL context and http clients
      retryPolicy = RetryPolicy[F](RetryPolicy.exponentialBackoff(30 seconds, 5))
      sslContext <- Resource.eval(SslContextReader.getSSLContext())
      httpClient <- client
        .BlazeClientBuilder[F]
        .withSslContext(sslContext)
        // Note a custom resolver is needed for making requests through the Leo proxy
        // (for example HttpJupyterDAO). Otherwise the proxyResolver falls back to default
        // hostname resolution, so it's okay to use for all clients.
        .withCustomDnsResolver(proxyResolver.resolveHttp4s)
        .resource
      httpClientWithLogging = Http4sLogger[F](logHeaders = true, logBody = false, logAction = Some(s => logAction(s)))(
        httpClient
      )
      httpClientWithRetryAndLogging = Retry(retryPolicy)(httpClientWithLogging)
      // Note the Sam client intentionally doesn't use httpClientWithLogging because the logs are
      // too verbose. We send OpenTelemetry metrics instead for instrumenting Sam calls.
      underlyingPetTokenCache = buildCache[UserEmailAndProject, scalacache.Entry[Option[String]]](
        httpSamDaoConfig.petCacheMaxSize,
        httpSamDaoConfig.petCacheExpiryTime
      )
      petTokenCache <- Resource.make(
        F.delay(CaffeineCache[F, UserEmailAndProject, Option[String]](underlyingPetTokenCache))
      )(s => F.delay(s.close))

      samDao = HttpSamDAO[F](httpClientWithRetryAndLogging, httpSamDaoConfig, petTokenCache)
      jupyterDao = new HttpJupyterDAO[F](runtimeDnsCache, httpClientWithLogging)
      welderDao = new HttpWelderDAO[F](runtimeDnsCache, httpClientWithLogging)
      rstudioDAO = new HttpRStudioDAO(runtimeDnsCache, httpClientWithLogging)
      appDAO = new HttpAppDAO(kubernetesDnsCache, httpClientWithLogging)
      dockerDao = HttpDockerDAO[F](httpClientWithRetryAndLogging)
      appDescriptorDAO = new HttpAppDescriptorDAO(httpClientWithRetryAndLogging)
      wsmDao = new HttpWsmDao[F](httpClientWithRetryAndLogging, ConfigReader.appConfig.azure.wsm)
      computeManagerDao = new HttpComputerManagerDao[F](ConfigReader.appConfig.azure.appRegistration)

      // Set up identity providers
      serviceAccountProvider = new PetClusterServiceAccountProvider(samDao)
      underlyingAuthCache = buildCache[AuthCacheKey, scalacache.Entry[Boolean]](samAuthConfig.authCacheMaxSize,
                                                                                samAuthConfig.authCacheExpiryTime)
      authCache <- Resource.make(F.delay(CaffeineCache[F, AuthCacheKey, Boolean](underlyingAuthCache)))(s =>
        F.delay(s.close)
      )
      authProvider = new SamAuthProvider(samDao, samAuthConfig, serviceAccountProvider, authCache)

      // Set up GCP credentials
      credential <- credentialResource(pathToCredentialJson)
      scopedCredential = credential.createScoped(Seq(ComputeScopes.COMPUTE).asJava)
      kubernetesScopedCredential = credential.createScoped(Seq(ContainerScopes.CLOUD_PLATFORM).asJava)
      credentialJson <- Resource.eval(
        readFileToString(applicationConfig.leoServiceAccountJsonFile)
      )
      json = Json(credentialJson)
      jsonWithServiceAccountUser = Json(credentialJson, Option(googleGroupsConfig.googleAdminEmail))

      // Set up Google DAOs
      googleIamDAO = new HttpGoogleIamDAO(applicationConfig.applicationName, json, workbenchMetricsBaseName)
      googleDirectoryDAO = new HttpGoogleDirectoryDAO(applicationConfig.applicationName,
                                                      jsonWithServiceAccountUser,
                                                      workbenchMetricsBaseName)
      googleResourceService <- GoogleResourceService.resource[F](Paths.get(pathToCredentialJson), semaphore)
      googleStorage <- GoogleStorageService.resource[F](pathToCredentialJson, Some(semaphore))
      googlePublisher <- GooglePublisher.resource[F](publisherConfig)
      cryptoMiningUserPublisher <- GooglePublisher.resource[F](cryptominingTopicPublisherConfig)
      gkeService <- GKEService.resource(Paths.get(pathToCredentialJson), semaphore)

      // Retry 400 responses from Google, as those can occur when resources aren't ready yet
      // (e.g. if the subnet isn't ready when creating an instance).
      googleComputeRetryPolicy = RetryPredicates.retryConfigWithPredicates(RetryPredicates.standardGoogleRetryPredicate,
                                                                           RetryPredicates.whenStatusCode(400))

      googleComputeService <- GoogleComputeService.fromCredential(
        scopedCredential,
        semaphore,
        googleComputeRetryPolicy
      )

      googleDataprocRetryPolicy = RetryPredicates.retryConfigWithPredicates(
        RetryPredicates.standardGoogleRetryPredicate,
        RetryPredicates.whenStatusCode(400)
      )

      computePollOperation <- ComputePollOperation.resourceFromCredential(scopedCredential, semaphore)
      dataprocService <- GoogleDataprocService
        .resource(
          googleComputeService,
          pathToCredentialJson,
          semaphore,
          dataprocConfig.supportedRegions,
          googleDataprocRetryPolicy
        )

      _ <- OpenTelemetryMetrics.registerTracing[F](Paths.get(pathToCredentialJson))

      googleDiskService <- GoogleDiskService.resource(pathToCredentialJson, semaphore)
      googleOauth2DAO <- GoogleOAuth2Service.resource(semaphore)
      underlyingNodepoolLockCache = buildCache[KubernetesClusterId, scalacache.Entry[Semaphore[F]]](
        gkeClusterConfig.nodepoolLockCacheMaxSize,
        gkeClusterConfig.nodepoolLockCacheExpiryTime
      )
      nodepoolLockCache <- Resource.make(
        F.delay(CaffeineCache[F, KubernetesClusterId, Semaphore[F]](underlyingNodepoolLockCache))
      )(s => F.delay(s.close))
      nodepoolLock = KeyLock[F, KubernetesClusterId](nodepoolLockCache)

      // Set up PubSub queues
      publisherQueue <- Resource.eval(Queue.bounded[F, LeoPubsubMessage](pubsubConfig.queueSize))
      leoPublisher = new LeoPublisher(publisherQueue, googlePublisher)
      dataAccessedUpdater <- Resource.eval(
        Queue.bounded[F, UpdateDateAccessMessage](dateAccessUpdaterConfig.queueSize)
      )
      subscriberQueue <- Resource.eval(Queue.bounded[F, Event[LeoPubsubMessage]](pubsubConfig.queueSize))
      subscriber <- GoogleSubscriber.resource(subscriberConfig, subscriberQueue)
      nonLeoMessageSubscriberQueue <- Resource.eval(
        Queue.bounded[F, Event[NonLeoMessage]](pubsubConfig.queueSize)
      )
      nonLeoMessageSubscriber <- GoogleSubscriber.resource(nonLeoMessageSubscriberConfig, nonLeoMessageSubscriberQueue)
      asyncTasksQueue <- Resource.eval(Queue.bounded[F, Task[F]](asyncTaskProcessorConfig.queueBound))

      // Set up k8s and helm clients
      underlyingKubeClientCache = buildCache[KubernetesClusterId, scalacache.Entry[ApiClient]](
        200,
        2 hours
      )
      kubeCache <- Resource.make(F.delay(CaffeineCache[F, KubernetesClusterId, ApiClient](underlyingKubeClientCache)))(
        s => F.delay(s.close)
      )
      kubeService <- org.broadinstitute.dsde.workbench.google2.KubernetesService
        .resource(Paths.get(pathToCredentialJson), gkeService, kubeCache)
      // Use a low concurrency for helm because it can generate very chatty network traffic
      // (especially for Galaxy) and cause issues at high concurrency.
      helmConcurrency <- Resource.eval(Semaphore[F](20L))
      helmClient = new HelmInterpreter[F](helmConcurrency)

      underlyingGoogleTokenCache = buildCache[String, scalacache.Entry[(UserInfo, Instant)]](
        proxyConfig.tokenCacheMaxSize,
        proxyConfig.tokenCacheExpiryTime
      )
      googleTokenCache <- Resource.make(
        F.delay(CaffeineCache[F, String, (UserInfo, Instant)](underlyingGoogleTokenCache))
      )(s => F.delay(s.close))

      underlyingSamResourceCache = buildCache[SamResourceCacheKey, scalacache.Entry[Option[String]]](
        proxyConfig.internalIdCacheMaxSize,
        proxyConfig.internalIdCacheExpiryTime
      )
      samResourceCache <- Resource.make(
        F.delay(CaffeineCache[F, SamResourceCacheKey, Option[String]](underlyingSamResourceCache))
      )(s => F.delay(s.close))
      recordMetricsProcesses = List(
        CacheMetrics("authCache").processWithUnderlyingCache(underlyingAuthCache),
        CacheMetrics("petTokenCache")
          .processWithUnderlyingCache(underlyingPetTokenCache),
        CacheMetrics("googleTokenCache")
          .processWithUnderlyingCache(underlyingGoogleTokenCache),
        CacheMetrics("samResourceCache")
          .processWithUnderlyingCache(underlyingSamResourceCache),
        CacheMetrics("runtimeDnsCache")
          .processWithUnderlyingCache(underlyingRuntimeDnsCache),
        CacheMetrics("kubernetesDnsCache")
          .processWithUnderlyingCache(underlyingKubernetesDnsCache),
        CacheMetrics("kubernetesApiClient")
          .processWithUnderlyingCache(underlyingKubeClientCache)
      )
    } yield {
      val googleDependencies = GoogleDependencies(
        googleStorage,
        googleComputeService,
        googleResourceService,
        googleDirectoryDAO,
        cryptoMiningUserPublisher,
        googleIamDAO,
        dataprocService,
        kubernetesDnsCache,
        gkeService,
        openTelemetry,
        kubernetesScopedCredential,
        googleOauth2DAO
      )

      val bucketHelperConfig = BucketHelperConfig(
        imageConfig,
        welderConfig,
        proxyConfig,
        securityFilesConfig
      )

      val bucketHelper = new BucketHelper[F](bucketHelperConfig, googleStorage, serviceAccountProvider)
      val vpcInterp = new VPCInterpreter(vpcInterpreterConfig, googleResourceService, googleComputeService)

      val dataprocInterp = new DataprocInterpreter(
        dataprocInterpreterConfig,
        bucketHelper,
        vpcInterp,
        dataprocService,
        googleComputeService,
        googleDiskService,
        googleDirectoryDAO,
        googleIamDAO,
        googleResourceService,
        welderDao
      )

      val gceInterp = new GceInterpreter(
        gceInterpreterConfig,
        bucketHelper,
        vpcInterp,
        googleDependencies.googleComputeService,
        googleDiskService,
        welderDao
      )

      implicit val runtimeInstances = new RuntimeInstances(dataprocInterp, gceInterp)

      val gkeAlg = new GKEInterpreter[F](
        gkeInterpConfig,
        vpcInterp,
        googleDependencies.gkeService,
        kubeService,
        helmClient,
        appDAO,
        googleDependencies.credentials,
        googleDependencies.googleIamDAO,
        googleDiskService,
        appDescriptorDAO,
        nodepoolLock,
        googleDependencies.googleResourceService
      )

      val azureAlg = new AzureInterpreter[F](ConfigReader.appConfig.azure.monitor,
                                             asyncTasksQueue,
                                             wsmDao,
                                             samDao,
                                             computeManagerDao)

      implicit val clusterToolToToolDao = ToolDAO.clusterToolToToolDao(jupyterDao, welderDao, rstudioDAO)
      val gceRuntimeMonitor = new GceRuntimeMonitor[F](
        gceMonitorConfig,
        googleDependencies.googleComputeService,
        authProvider,
        googleDependencies.googleStorageService,
        publisherQueue,
        gceInterp
      )

      val dataprocRuntimeMonitor = new DataprocRuntimeMonitor[F](
        dataprocMonitorConfig,
        googleDependencies.googleComputeService,
        authProvider,
        googleDependencies.googleStorageService,
        dataprocInterp,
        googleDependencies.googleDataproc
      )

      implicit val cloudServiceRuntimeMonitor =
        new CloudServiceRuntimeMonitor(gceRuntimeMonitor, dataprocRuntimeMonitor)

      val pubsubSubscriber = new LeoPubsubMessageSubscriber[F](
        leoPubsubMessageSubscriberConfig,
        subscriber,
        asyncTasksQueue,
        googleDiskService,
        authProvider,
        gkeAlg,
        azureAlg
      )

      AppDependencies(
        sslContext,
        dbRef,
        runtimeDnsCache,
        googleDependencies,
        samDao,
        dockerDao,
        jupyterDao,
        wsmDao,
        serviceAccountProvider,
        authProvider,
        leoPublisher,
        publisherQueue,
        dataAccessedUpdater,
        subscriber,
        nonLeoMessageSubscriber,
        asyncTasksQueue,
        nodepoolLock,
        proxyResolver,
        recordMetricsProcesses,
        googleTokenCache,
        samResourceCache,
        pubsubSubscriber,
        gkeAlg,
        dataprocInterp
      )
    }

  private def logAction[F[_]: Monad: StructuredLogger](s: String): F[Unit] =
    StructuredLogger[F].info(s)

  private def buildCache[K, V](maxSize: Int,
                               expiresIn: FiniteDuration): com.github.benmanes.caffeine.cache.Cache[K, V] =
    Caffeine
      .newBuilder()
      .maximumSize(maxSize)
      .expireAfterWrite(expiresIn.toSeconds, TimeUnit.SECONDS)
      .recordStats()
      .build[K, V]()

  override def run(args: List[String]): IO[ExitCode] = startup().as(ExitCode.Success)
}

final case class GoogleDependencies[F[_]](
  googleStorageService: GoogleStorageService[F],
  googleComputeService: GoogleComputeService[F],
  googleResourceService: GoogleResourceService[F],
  googleDirectoryDAO: HttpGoogleDirectoryDAO,
  cryptoMiningUserPublisher: GooglePublisher[F],
  googleIamDAO: HttpGoogleIamDAO,
  googleDataproc: GoogleDataprocService[F],
  kubernetesDnsCache: KubernetesDnsCache[F],
  gkeService: GKEService[F],
  openTelemetryMetrics: OpenTelemetryMetrics[F],
  credentials: GoogleCredentials,
  googleOauth2DAO: GoogleOAuth2Service[F]
)

final case class AppDependencies[F[_]](
  sslContext: SSLContext,
  dbReference: DbReference[F],
  runtimeDnsCache: RuntimeDnsCache[F],
  googleDependencies: GoogleDependencies[F],
  samDAO: HttpSamDAO[F],
  dockerDAO: HttpDockerDAO[F],
  jupyterDAO: HttpJupyterDAO[F],
  wsmDAO: HttpWsmDao[F],
  serviceAccountProvider: ServiceAccountProvider[F],
  authProvider: SamAuthProvider[F],
  leoPublisher: LeoPublisher[F],
  publisherQueue: Queue[F, LeoPubsubMessage],
  dateAccessedUpdaterQueue: Queue[F, UpdateDateAccessMessage],
  subscriber: GoogleSubscriber[F, LeoPubsubMessage],
  nonLeoMessageGoogleSubscriber: GoogleSubscriber[F, NonLeoMessage],
  asyncTasksQueue: Queue[F, Task[F]],
  nodepoolLock: KeyLock[F, KubernetesClusterId],
  proxyResolver: ProxyResolver[F],
  recordCacheMetrics: List[Stream[F, Unit]],
  googleTokenCache: scalacache.Cache[F, String, (UserInfo, Instant)],
  samResourceCache: scalacache.Cache[F, SamResourceCacheKey, Option[String]],
  pubsubSubscriber: LeoPubsubMessageSubscriber[F],
  gkeAlg: GKEAlgebra[F],
  dataprocInterp: DataprocInterpreter[F]
)
