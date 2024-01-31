package org.broadinstitute.dsde.workbench.leonardo
package http

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import cats.effect._
import cats.effect.std.{Dispatcher, Queue, Semaphore}
import cats.mtl.Ask
import cats.syntax.all._
import cats.{Monad, Parallel}
import com.github.benmanes.caffeine.cache.Caffeine
import com.google.api.gax.longrunning.OperationFuture
import com.google.api.services.container.ContainerScopes
import com.google.auth.oauth2.GoogleCredentials
import com.google.cloud.compute.v1.Operation
import fs2.io.file.Files
import fs2.Stream
import io.circe.syntax._
import io.kubernetes.client.openapi.ApiClient
import org.broadinstitute.dsde.workbench.azure.{
  AzureApplicationInsightsService,
  AzureBatchService,
  AzureContainerService,
  AzureRelayService,
  AzureVmService
}
import org.broadinstitute.dsde.workbench.google.GoogleCredentialModes.Json
import org.broadinstitute.dsde.workbench.google.{
  GoogleProjectDAO,
  HttpGoogleDirectoryDAO,
  HttpGoogleIamDAO,
  HttpGoogleProjectDAO
}
import org.broadinstitute.dsde.workbench.google2.GKEModels.KubernetesClusterId
import org.broadinstitute.dsde.workbench.google2.util.RetryPredicates
import org.broadinstitute.dsde.workbench.google2.{
  credentialResource,
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
import org.broadinstitute.dsde.workbench.leonardo.app.{
  AppInstall,
  CromwellAppInstall,
  CromwellRunnerAppInstall,
  HailBatchAppInstall,
  WdsAppInstall,
  WorkflowsAppInstall
}
import org.broadinstitute.dsde.workbench.leonardo.auth.{AuthCacheKey, PetClusterServiceAccountProvider, SamAuthProvider}
import org.broadinstitute.dsde.workbench.leonardo.config.Config._
import org.broadinstitute.dsde.workbench.leonardo.config.LeoExecutionModeConfig
import org.broadinstitute.dsde.workbench.leonardo.dao._
import org.broadinstitute.dsde.workbench.leonardo.dao.google.GoogleOAuth2Service
import org.broadinstitute.dsde.workbench.leonardo.db.DbReference
import org.broadinstitute.dsde.workbench.leonardo.dns._
import org.broadinstitute.dsde.workbench.leonardo.http.api.{
  BuildTimeVersion,
  HttpRoutes,
  LivenessRoutes,
  StandardUserInfoDirectives
}
import org.broadinstitute.dsde.workbench.leonardo.http.service._
import org.broadinstitute.dsde.workbench.leonardo.model.ServiceAccountProvider
import org.broadinstitute.dsde.workbench.leonardo.monitor.LeoPubsubCodec.leoPubsubMessageDecoder
import org.broadinstitute.dsde.workbench.leonardo.monitor.NonLeoMessageSubscriber.nonLeoMessageDecoder
import org.broadinstitute.dsde.workbench.leonardo.monitor._
import org.broadinstitute.dsde.workbench.leonardo.util._
import org.broadinstitute.dsde.workbench.model.{IP, TraceId, UserInfo}
import org.broadinstitute.dsde.workbench.oauth2.OpenIDConnectConfiguration
import org.broadinstitute.dsde.workbench.openTelemetry.OpenTelemetryMetrics
import org.broadinstitute.dsp.HelmInterpreter
import org.http4s.Request
import org.http4s.blaze.client
import org.http4s.client.RequestKey
import org.http4s.client.middleware.{Logger => Http4sLogger, Metrics, Retry, RetryPolicy}
import org.typelevel.log4cats.StructuredLogger
import org.typelevel.log4cats.slf4j.Slf4jLogger
import scalacache.caffeine._
import java.net.{InetSocketAddress, SocketException}
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

    val livenessRoutes = new LivenessRoutes

    logger
      .info("Liveness server has been created, starting...")
      .unsafeToFuture()(cats.effect.unsafe.IORuntime.global) >> Http()
      .newServerAt("0.0.0.0", 9000)
      .bindFlow(livenessRoutes.route)
      .onError { case t: Throwable =>
        logger
          .error(t)("FATAL - failure starting liveness http server")
          .unsafeToFuture()(cats.effect.unsafe.IORuntime.global)
      }

    logger.info("Liveness server has been started").unsafeRunSync()(cats.effect.unsafe.IORuntime.global)

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
          // For now azure disks share same defaults as normal disks
          ConfigReader.appConfig.persistentDisk,
          ConfigReader.appConfig.azure.pubsubHandler.runtimeDefaults.image,
          ConfigReader.appConfig.azure.pubsubHandler.runtimeDefaults.listenerImage,
          ConfigReader.appConfig.azure.pubsubHandler.welderImage
        )
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
        appDependencies.publisherQueue,
        googleDependencies.googleDiskService,
        googleDependencies.googleProjectDAO
      )

      val diskV2Service = new DiskV2ServiceInterp[IO](
        ConfigReader.appConfig.persistentDisk,
        appDependencies.authProvider,
        appDependencies.wsmDAO,
        appDependencies.samDAO,
        appDependencies.publisherQueue
      )

      val leoKubernetesService: LeoAppServiceInterp[IO] =
        new LeoAppServiceInterp(
          appServiceConfig,
          appDependencies.authProvider,
          appDependencies.serviceAccountProvider,
          appDependencies.publisherQueue,
          appDependencies.googleDependencies.googleComputeService,
          googleDependencies.googleResourceService,
          gkeCustomAppConfig,
          appDependencies.wsmDAO
        )

      val azureService = new RuntimeV2ServiceInterp[IO](
        runtimeServiceConfig,
        appDependencies.authProvider,
        appDependencies.wsmDAO,
        appDependencies.publisherQueue,
        appDependencies.dateAccessedUpdaterQueue,
        appDependencies.wsmClientProvider
      )

      val adminService = new AdminServiceInterp[IO](
        appDependencies.authProvider,
        appDependencies.publisherQueue
      )

      val resourcesService = new ResourcesServiceInterp[IO](
        appDependencies.authProvider
      )

      val httpRoutes = new HttpRoutes(
        appDependencies.openIDConnectConfiguration,
        statusService,
        proxyService,
        runtimeService,
        diskService,
        diskV2Service,
        leoKubernetesService,
        azureService,
        adminService,
        resourcesService,
        StandardUserInfoDirectives,
        contentSecurityPolicy,
        refererConfig
      )
      val httpServer = for {
        start <- IO.realTimeInstant
        implicit0(ctx: Ask[IO, AppContext]) = Ask.const[IO, AppContext](
          AppContext(TraceId(s"Boot_${start}"), start)
        )
        // This only needs to happen once in each environment
        _ <- appDependencies.samDAO.registerLeo.handleErrorWith { case e =>
          logger.warn(e)("fail to register Leonardo SA")
        }
        _ <-
          if (leoExecutionModeConfig == LeoExecutionModeConfig.BackLeoOnly) {
            appDependencies.dataprocInterp.setupDataprocImageGoogleGroup
          } else IO.unit

        _ <- IO.fromFuture {
          IO {
            Http()
              .newServerAt("0.0.0.0", 8080)
              .bindFlow(httpRoutes.route)
              .onError { case t: Throwable =>
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
            new MonitorAtBoot[IO](
              appDependencies.publisherQueue,
              googleDependencies.googleComputeService,
              appDependencies.samDAO,
              appDependencies.wsmDAO
            )

          val autopauseMonitorProcess = AutopauseMonitor.process(
            autoFreezeConfig,
            appDependencies.jupyterDAO,
            appDependencies.publisherQueue
          )

          val nonLeoMessageSubscriber =
            new NonLeoMessageSubscriber[IO](
              NonLeoMessageSubscriberConfig(gceConfig.userDiskDeviceName),
              appDependencies.gkeAlg,
              googleDependencies.googleComputeService,
              appDependencies.samDAO,
              appDependencies.authProvider,
              appDependencies.nonLeoMessageGoogleSubscriber,
              googleDependencies.cryptoMiningUserPublisher,
              appDependencies.asyncTasksQueue
            )

          // LeoMetricsMonitor collects metrics from both runtimes and apps.
          // - clusterToolToToolDao provides jupyter/rstudio/welder DAOs for runtime status checking.
          // - appDAO, wdsDAO, cbasDAO, cromwellDAO are for status checking apps.
          implicit val clusterToolToToolDao =
            ToolDAO.clusterToolToToolDao(appDependencies.jupyterDAO,
                                         appDependencies.welderDAO,
                                         appDependencies.rstudioDAO
            )
          val metricsMonitor = new LeoMetricsMonitor(
            ConfigReader.appConfig.metrics,
            appDependencies.appDAO,
            appDependencies.wdsDAO,
            appDependencies.cbasDAO,
            appDependencies.cromwellDAO,
            appDependencies.hailBatchDAO,
            appDependencies.listenerDAO,
            appDependencies.samDAO,
            appDependencies.kubeAlg,
            appDependencies.azureContainerService
          )

          List(
            nonLeoMessageSubscriber.process,
            Stream.eval(appDependencies.nonLeoMessageGoogleSubscriber.start),
            asyncTasks.process,
            appDependencies.pubsubSubscriber.process,
            Stream.eval(appDependencies.subscriber.start),
            monitorAtBoot.process, // checks database to see if there's on-going runtime status transition
            autopauseMonitorProcess, // check database to autopause runtimes periodically
            metricsMonitor.process // checks database and collects metrics about active runtimes and apps
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
          appDependencies.leoPublisher.process, // start the publisher queue .dequeue
          Stream.eval[IO, Unit](httpServer) // start http server
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
  )(implicit
    logger: StructuredLogger[F],
    ec: ExecutionContext,
    as: ActorSystem,
    F: Async[F],
    files: Files[F]
  ): Resource[F, AppDependencies[F]] =
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
      hostToIpMapping <- Resource.eval(Ref.of(Map.empty[String, IP]))
      proxyResolver <- Dispatcher.parallel[F].map(d => ProxyResolver(hostToIpMapping, d))

      underlyingRuntimeDnsCache = buildCache[RuntimeDnsCacheKey, scalacache.Entry[HostStatus]](
        runtimeDnsCacheConfig.cacheMaxSize,
        runtimeDnsCacheConfig.cacheExpiryTime
      )
      runtimeDnsCaffeineCache <- Resource.make(
        F.delay(CaffeineCache[F, RuntimeDnsCacheKey, HostStatus](underlyingRuntimeDnsCache))
      )(_.close)
      runtimeDnsCache = new RuntimeDnsCache(proxyConfig, dbRef, hostToIpMapping, runtimeDnsCaffeineCache)
      underlyingKubernetesDnsCache = buildCache[KubernetesDnsCacheKey, scalacache.Entry[HostStatus]](
        kubernetesDnsCacheConfig.cacheMaxSize,
        kubernetesDnsCacheConfig.cacheExpiryTime
      )

      kubernetesDnsCaffineCache <- Resource.make(
        F.delay(CaffeineCache[F, KubernetesDnsCacheKey, HostStatus](underlyingKubernetesDnsCache))
      )(_.close)
      kubernetesDnsCache = new KubernetesDnsCache(proxyConfig, dbRef, hostToIpMapping, kubernetesDnsCaffineCache)

      // Set up SSL context and http clients
      sslContext <- Resource.eval(SslContextReader.getSSLContext())
      underlyingPetKeyCache = buildCache[UserEmailAndProject, scalacache.Entry[Option[io.circe.Json]]](
        httpSamDaoConfig.petCacheMaxSize,
        httpSamDaoConfig.petCacheExpiryTime
      )
      petKeyCache <- Resource.make(
        F.delay(CaffeineCache[F, UserEmailAndProject, Option[io.circe.Json]](underlyingPetKeyCache))
      )(_.close)

      samDao <- buildHttpClient(sslContext, proxyResolver.resolveHttp4s, Some("leo_sam_client"), true).map(client =>
        HttpSamDAO[F](client, httpSamDaoConfig, petKeyCache)
      )
      cromwellDao <- buildHttpClient(sslContext, proxyResolver.resolveHttp4s, Some("leo_cromwell_client"), false).map(
        client => new HttpCromwellDAO[F](client)
      )
      cbasDao <- buildHttpClient(sslContext, proxyResolver.resolveHttp4s, Some("leo_cbas_client"), false).map(client =>
        new HttpCbasDAO[F](client)
      )
      wdsDao <- buildHttpClient(sslContext, proxyResolver.resolveHttp4s, Some("leo_wds_client"), false).map(client =>
        new HttpWdsDAO[F](client)
      )
      hailBatchDao <- buildHttpClient(sslContext, proxyResolver.resolveHttp4s, Some("leo_hail_batch_client"), false)
        .map(client => new HttpHailBatchDAO[F](client))
      listenerDao <- buildHttpClient(sslContext, proxyResolver.resolveHttp4s, Some("leo_listener_client"), false).map(
        client => new HttpListenerDAO[F](client)
      )
      jupyterDao <- buildHttpClient(sslContext, proxyResolver.resolveHttp4s, Some("leo_jupyter_client"), false).map(
        client => new HttpJupyterDAO[F](runtimeDnsCache, client, samDao)
      )
      welderDao <- buildHttpClient(sslContext, proxyResolver.resolveHttp4s, Some("leo_welder_client"), false).map(
        client => new HttpWelderDAO[F](runtimeDnsCache, client, samDao)
      )
      rstudioDAO <- buildHttpClient(sslContext, proxyResolver.resolveHttp4s, Some("leo_rstudio_client"), false).map(
        client => new HttpRStudioDAO(runtimeDnsCache, client)
      )
      appDAO <- buildHttpClient(sslContext, proxyResolver.resolveHttp4s, Some("leo_app_client"), false).map(client =>
        new HttpAppDAO(kubernetesDnsCache, client)
      )
      dockerDao <- buildHttpClient(sslContext, proxyResolver.resolveHttp4s, None, true).map(client =>
        HttpDockerDAO[F](client)
      )
      appDescriptorDAO <- buildHttpClient(sslContext, proxyResolver.resolveHttp4s, None, true).map(client =>
        new HttpAppDescriptorDAO(client)
      )
      wsmDao <- buildHttpClient(sslContext, proxyResolver.resolveHttp4s, Some("leo_wsm_client"), true)
        .map(client => new HttpWsmDao[F](client, ConfigReader.appConfig.azure.wsm))
      googleOauth2DAO <- GoogleOAuth2Service.resource(semaphore)

      wsmClientProvider = new HttpWsmClientProvider(ConfigReader.appConfig.azure.wsm.uri)

      azureRelay <- AzureRelayService.fromAzureAppRegistrationConfig(ConfigReader.appConfig.azure.appRegistration)
      azureVmService <- AzureVmService.fromAzureAppRegistrationConfig(ConfigReader.appConfig.azure.appRegistration)
      azureContainerService <- AzureContainerService.fromAzureAppRegistrationConfig(
        ConfigReader.appConfig.azure.appRegistration
      )

      azureBatchService <- AzureBatchService.fromAzureAppRegistrationConfig(
        ConfigReader.appConfig.azure.appRegistration
      )

      azureApplicationInsightsService <- AzureApplicationInsightsService.fromAzureAppRegistrationConfig(
        ConfigReader.appConfig.azure.appRegistration
      )
      // Set up identity providers
      serviceAccountProvider = new PetClusterServiceAccountProvider(samDao)
      underlyingAuthCache = buildCache[AuthCacheKey, scalacache.Entry[Boolean]](samAuthConfig.authCacheMaxSize,
                                                                                samAuthConfig.authCacheExpiryTime
      )
      authCache <- Resource.make(F.delay(CaffeineCache[F, AuthCacheKey, Boolean](underlyingAuthCache)))(s => s.close)
      authProvider = new SamAuthProvider(samDao, samAuthConfig, serviceAccountProvider, authCache)

      // Set up GCP credentials
      credential <- credentialResource(pathToCredentialJson)
      scopedCredential = credential.createScoped(Seq("https://www.googleapis.com/auth/compute").asJava)
      kubernetesScopedCredential = credential.createScoped(Seq(ContainerScopes.CLOUD_PLATFORM).asJava)
      credentialJson <- Resource.eval(
        readFileToString(applicationConfig.leoServiceAccountJsonFile)
      )
      json = Json(credentialJson)
      jsonWithServiceAccountUser = Json(credentialJson, Option(googleGroupsConfig.googleAdminEmail))

      // Set up Google DAOs
      googleProjectDAO = new HttpGoogleProjectDAO(applicationConfig.applicationName, json, workbenchMetricsBaseName)
      googleIamDAO = new HttpGoogleIamDAO(applicationConfig.applicationName, json, workbenchMetricsBaseName)
      googleDirectoryDAO = new HttpGoogleDirectoryDAO(applicationConfig.applicationName,
                                                      jsonWithServiceAccountUser,
                                                      workbenchMetricsBaseName
      )
      googleResourceService <- GoogleResourceService.resource[F](Paths.get(pathToCredentialJson), semaphore)
      googleStorage <- GoogleStorageService.resource[F](pathToCredentialJson, Some(semaphore))
      googlePublisher <- GooglePublisher.resource[F](publisherConfig)
      cryptoMiningUserPublisher <- GooglePublisher.resource[F](cryptominingTopicPublisherConfig)
      gkeService <- GKEService.resource(Paths.get(pathToCredentialJson), semaphore)

      // Retry 400 responses from Google, as those can occur when resources aren't ready yet
      // (e.g. if the subnet isn't ready when creating an instance).
      googleComputeRetryPolicy = RetryPredicates.retryConfigWithPredicates(RetryPredicates.standardGoogleRetryPredicate,
                                                                           RetryPredicates.whenStatusCode(400)
      )

      googleComputeService <- GoogleComputeService.fromCredential(
        scopedCredential,
        semaphore,
        googleComputeRetryPolicy
      )

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

      underlyingNodepoolLockCache = buildCache[KubernetesClusterId, scalacache.Entry[Semaphore[F]]](
        gkeClusterConfig.nodepoolLockCacheMaxSize,
        gkeClusterConfig.nodepoolLockCacheExpiryTime
      )
      nodepoolLockCache <- Resource.make(
        F.delay(CaffeineCache[F, KubernetesClusterId, Semaphore[F]](underlyingNodepoolLockCache))
      )(_.close)
      nodepoolLock = KeyLock[F, KubernetesClusterId](nodepoolLockCache)

      // Set up PubSub queues
      publisherQueue <- Resource.eval(Queue.bounded[F, LeoPubsubMessage](pubsubConfig.queueSize))
      leoPublisher = new LeoPublisher(publisherQueue, googlePublisher)
      dataAccessedUpdater <- Resource.eval(
        Queue.bounded[F, UpdateDateAccessedMessage](dateAccessUpdaterConfig.queueSize)
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
        _.close
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
      )(_.close)

      underlyingSamResourceCache = buildCache[SamResourceCacheKey,
                                              scalacache.Entry[(Option[String], Option[AppAccessScope])]
      ](
        proxyConfig.internalIdCacheMaxSize,
        proxyConfig.internalIdCacheExpiryTime
      )
      samResourceCache <- Resource.make(
        F.delay(
          CaffeineCache[F, SamResourceCacheKey, (Option[String], Option[AppAccessScope])](underlyingSamResourceCache)
        )
      )(s => s.close)

      underlyingOperationFutureCache = buildCache[Long, scalacache.Entry[OperationFuture[Operation, Operation]]](
        500,
        5 minutes
      )
      operationFutureCache <- Resource.make(
        F.delay(CaffeineCache[F, Long, OperationFuture[Operation, Operation]](underlyingOperationFutureCache))
      )(_.close)

      oidcConfig <- Resource.eval(
        OpenIDConnectConfiguration[F](
          ConfigReader.appConfig.oidc.authorityEndpoint.renderString,
          ConfigReader.appConfig.oidc.clientId,
          oidcClientSecret = ConfigReader.appConfig.oidc.clientSecret,
          extraGoogleClientId = Some(ConfigReader.appConfig.oidc.legacyGoogleClientId),
          extraAuthParams = Some("prompt=login")
        )
      )

      recordMetricsProcesses = List(
        CacheMetrics("authCache").processWithUnderlyingCache(underlyingAuthCache),
        CacheMetrics("petTokenCache")
          .processWithUnderlyingCache(underlyingPetKeyCache),
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
        googleOauth2DAO,
        googleDiskService,
        googleProjectDAO
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

      val kubeAlg = new KubernetesInterpreter[F](
        azureContainerService,
        googleDependencies.gkeService,
        googleDependencies.credentials
      )

      val cromwellAppInstall = new CromwellAppInstall[F](
        ConfigReader.appConfig.azure.coaAppConfig,
        ConfigReader.appConfig.drs,
        samDao,
        cromwellDao,
        cbasDao,
        azureBatchService,
        azureApplicationInsightsService
      )
      val cromwellRunnerAppInstall =
        new CromwellRunnerAppInstall[F](ConfigReader.appConfig.azure.cromwellRunnerAppConfig,
                                        ConfigReader.appConfig.drs,
                                        samDao,
                                        cromwellDao,
                                        azureBatchService,
                                        azureApplicationInsightsService
        )
      val hailBatchAppInstall =
        new HailBatchAppInstall[F](ConfigReader.appConfig.azure.hailBatchAppConfig, hailBatchDao)
      val wdsAppInstall = new WdsAppInstall[F](ConfigReader.appConfig.azure.wdsAppConfig,
                                               ConfigReader.appConfig.azure.tdr,
                                               samDao,
                                               wdsDao,
                                               azureApplicationInsightsService
      )
      val workflowsAppInstall =
        new WorkflowsAppInstall[F](
          ConfigReader.appConfig.azure.workflowsAppConfig,
          ConfigReader.appConfig.drs,
          samDao,
          cromwellDao,
          cbasDao,
          azureBatchService,
          azureApplicationInsightsService
        )

      implicit val appTypeToAppInstall = AppInstall.appTypeToAppInstall(wdsAppInstall,
                                                                        cromwellAppInstall,
                                                                        workflowsAppInstall,
                                                                        hailBatchAppInstall,
                                                                        cromwellRunnerAppInstall
      )

      val gkeAlg = new GKEInterpreter[F](
        gkeInterpConfig,
        bucketHelper,
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
        googleDependencies.googleResourceService,
        googleDependencies.googleComputeService
      )

      val aksAlg = new AKSInterpreter[F](
        AKSInterpreterConfig(
          samConfig,
          appMonitorConfig,
          ConfigReader.appConfig.azure.wsm,
          applicationConfig.leoUrlBase,
          ConfigReader.appConfig.azure.pubsubHandler.runtimeDefaults.listenerImage,
          ConfigReader.appConfig.azure.listenerChartConfig
        ),
        helmClient,
        azureContainerService,
        azureRelay,
        samDao,
        wsmDao,
        kubeAlg,
        wsmClientProvider,
        wsmDao
      )

      val azureAlg = new AzurePubsubHandlerInterp[F](
        ConfigReader.appConfig.azure.pubsubHandler,
        applicationConfig,
        contentSecurityPolicy,
        asyncTasksQueue,
        wsmDao,
        samDao,
        welderDao,
        jupyterDao,
        azureRelay,
        azureVmService,
        aksAlg,
        refererConfig
      )

      implicit val clusterToolToToolDao = ToolDAO.clusterToolToToolDao(jupyterDao, welderDao, rstudioDAO)
      val gceRuntimeMonitor = new GceRuntimeMonitor[F](
        gceMonitorConfig,
        googleDependencies.googleComputeService,
        authProvider,
        googleDependencies.googleStorageService,
        googleDependencies.googleDiskService,
        publisherQueue,
        gceInterp
      )

      val dataprocRuntimeMonitor = new DataprocRuntimeMonitor[F](
        dataprocMonitorConfig,
        googleDependencies.googleComputeService,
        authProvider,
        googleDependencies.googleStorageService,
        googleDependencies.googleDiskService,
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
        azureAlg,
        operationFutureCache
      )

      AppDependencies(
        sslContext,
        dbRef,
        runtimeDnsCache,
        googleDependencies,
        samDao,
        dockerDao,
        jupyterDao,
        rstudioDAO,
        welderDao,
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
        dataprocInterp,
        oidcConfig,
        aksAlg,
        appDAO,
        wdsDao,
        cbasDao,
        cromwellDao,
        hailBatchDao,
        listenerDao,
        wsmClientProvider,
        kubeAlg,
        azureContainerService
      )
    }

  private def logAction[F[_]: Monad: StructuredLogger](s: String): F[Unit] =
    StructuredLogger[F].info(s)

  private def buildCache[K, V](maxSize: Int,
                               expiresIn: FiniteDuration
  ): com.github.benmanes.caffeine.cache.Cache[K, V] =
    Caffeine
      .newBuilder()
      .maximumSize(maxSize)
      .expireAfterWrite(expiresIn.toSeconds, TimeUnit.SECONDS)
      .recordStats()
      .build[K, V]()

  private def buildHttpClient[F[_]: Async: StructuredLogger](
    sslContext: SSLContext,
    dnsResolver: RequestKey => Either[Throwable, InetSocketAddress],
    metricsPrefix: Option[String],
    withRetry: Boolean
  ): Resource[F, org.http4s.client.Client[F]] = {
    // Retry all SocketExceptions to deal with pooled HTTP connections getting closed.
    // See https://broadworkbench.atlassian.net/browse/IA-4069.
    val retryPolicy = RetryPolicy[F](
      RetryPolicy.exponentialBackoff(30 seconds, 5),
      (req, result) =>
        result match {
          case Left(e) if e.isInstanceOf[SocketException] => true
          case _                                          => RetryPolicy.defaultRetriable(req, result)
        }
    )

    for {
      httpClient <- client
        .BlazeClientBuilder[F]
        .withSslContext(sslContext)
        // Note a custom resolver is needed for making requests through the Leo proxy
        // (for example HttpJupyterDAO). Otherwise the proxyResolver falls back to default
        // hostname resolution, so it's okay to use for all clients.
        .withCustomDnsResolver(dnsResolver)
        .withConnectTimeout(30 seconds)
        .withRequestTimeout(60 seconds)
        .withMaxTotalConnections(100)
        .withMaxWaitQueueLimit(1024)
        .withMaxIdleDuration(30 seconds)
        .resource
      httpClientWithLogging = Http4sLogger[F](logHeaders = true, logBody = false, logAction = Some(s => logAction(s)))(
        httpClient
      )
      clientWithRetry = if (withRetry) Retry(retryPolicy)(httpClientWithLogging) else httpClientWithLogging
      finalClient <- metricsPrefix match {
        case None => Resource.pure[F, org.http4s.client.Client[F]](clientWithRetry)
        case Some(prefix) =>
          val classifierFunc = (r: Request[F]) => Some(r.method.toString.toLowerCase)
          for {
            metricsOps <- org.http4s.metrics.prometheus.Prometheus
              .metricsOps(io.prometheus.client.CollectorRegistry.defaultRegistry, prefix)
            meteredClient = Metrics[F](
              metricsOps,
              classifierFunc
            )(clientWithRetry)
          } yield meteredClient
      }
    } yield finalClient
  }

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
  googleOauth2DAO: GoogleOAuth2Service[F],
  googleDiskService: GoogleDiskService[F],
  googleProjectDAO: GoogleProjectDAO
)

final case class AppDependencies[F[_]](
  sslContext: SSLContext,
  dbReference: DbReference[F],
  runtimeDnsCache: RuntimeDnsCache[F],
  googleDependencies: GoogleDependencies[F],
  samDAO: HttpSamDAO[F],
  dockerDAO: HttpDockerDAO[F],
  jupyterDAO: HttpJupyterDAO[F],
  rstudioDAO: HttpRStudioDAO[F],
  welderDAO: HttpWelderDAO[F],
  wsmDAO: HttpWsmDao[F],
  serviceAccountProvider: ServiceAccountProvider[F],
  authProvider: SamAuthProvider[F],
  leoPublisher: LeoPublisher[F],
  publisherQueue: Queue[F, LeoPubsubMessage],
  dateAccessedUpdaterQueue: Queue[F, UpdateDateAccessedMessage],
  subscriber: GoogleSubscriber[F, LeoPubsubMessage],
  nonLeoMessageGoogleSubscriber: GoogleSubscriber[F, NonLeoMessage],
  asyncTasksQueue: Queue[F, Task[F]],
  nodepoolLock: KeyLock[F, KubernetesClusterId],
  proxyResolver: ProxyResolver[F],
  recordCacheMetrics: List[Stream[F, Unit]],
  googleTokenCache: scalacache.Cache[F, String, (UserInfo, Instant)],
  samResourceCache: scalacache.Cache[F, SamResourceCacheKey, (Option[String], Option[AppAccessScope])],
  pubsubSubscriber: LeoPubsubMessageSubscriber[F],
  gkeAlg: GKEAlgebra[F],
  dataprocInterp: DataprocInterpreter[F],
  openIDConnectConfiguration: OpenIDConnectConfiguration,
  aksInterp: AKSAlgebra[F],
  appDAO: AppDAO[F],
  wdsDAO: WdsDAO[F],
  cbasDAO: CbasDAO[F],
  cromwellDAO: CromwellDAO[F],
  hailBatchDAO: HailBatchDAO[F],
  listenerDAO: ListenerDAO[F],
  wsmClientProvider: HttpWsmClientProvider[F],
  kubeAlg: KubernetesAlgebra[F],
  azureContainerService: AzureContainerService[F]
)
