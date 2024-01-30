package org.broadinstitute.dsde.workbench.leonardo
package http

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import cats.Monad
import cats.effect._
import cats.effect.std.{Dispatcher, Queue, Semaphore}
import cats.mtl.Ask
import cats.syntax.all._
import com.github.benmanes.caffeine.cache.Caffeine
import com.google.api.gax.longrunning.OperationFuture
import com.google.api.services.container.ContainerScopes
import com.google.cloud.compute.v1.Operation
import fs2.Stream
import io.circe.syntax._
import io.kubernetes.client.openapi.ApiClient
import org.broadinstitute.dsde.workbench.azure._
import org.broadinstitute.dsde.workbench.google.GoogleCredentialModes.Json
import org.broadinstitute.dsde.workbench.google.{GoogleProjectDAO, HttpGoogleDirectoryDAO, HttpGoogleIamDAO, HttpGoogleProjectDAO}
import org.broadinstitute.dsde.workbench.google2.GKEModels.KubernetesClusterId
import org.broadinstitute.dsde.workbench.google2.util.RetryPredicates
import org.broadinstitute.dsde.workbench.google2.{GKEService, GoogleComputeService, GoogleDataprocService, GoogleDiskService, GooglePublisher, GoogleResourceService, GoogleStorageService, GoogleSubscriber, credentialResource}
import org.broadinstitute.dsde.workbench.leonardo.AsyncTaskProcessor.Task
import org.broadinstitute.dsde.workbench.leonardo.LeoPublisher
import org.broadinstitute.dsde.workbench.leonardo.app._
import org.broadinstitute.dsde.workbench.leonardo.auth.{AuthCacheKey, PetClusterServiceAccountProvider, SamAuthProvider}
import org.broadinstitute.dsde.workbench.leonardo.config.Config._
import org.broadinstitute.dsde.workbench.leonardo.config.LeoExecutionModeConfig
import org.broadinstitute.dsde.workbench.leonardo.dao._
import org.broadinstitute.dsde.workbench.leonardo.dao.google.GoogleOAuth2Service
import org.broadinstitute.dsde.workbench.leonardo.db.DbReference
import org.broadinstitute.dsde.workbench.leonardo.dns._
import org.broadinstitute.dsde.workbench.leonardo.http.api.{BuildTimeVersion, GCPModeSpecificServices, HttpRoutes, LivenessRoutes, StandardUserInfoDirectives}
import org.broadinstitute.dsde.workbench.leonardo.http.service._
import org.broadinstitute.dsde.workbench.leonardo.model.ServiceAccountProvider
import org.broadinstitute.dsde.workbench.leonardo.monitor.LeoPubsubCodec.leoPubsubMessageDecoder
import org.broadinstitute.dsde.workbench.leonardo.monitor.NonLeoMessageSubscriber.nonLeoMessageDecoder
import org.broadinstitute.dsde.workbench.leonardo.monitor._
import org.broadinstitute.dsde.workbench.leonardo.util._
import org.broadinstitute.dsde.workbench.model.{IP, TraceId, UserInfo}
import org.broadinstitute.dsde.workbench.oauth2.OpenIDConnectConfiguration
import org.broadinstitute.dsde.workbench.openTelemetry.OpenTelemetryMetrics
import org.broadinstitute.dsde.workbench.util2.messaging._
import org.broadinstitute.dsp.HelmInterpreter
import org.http4s.Request
import org.http4s.blaze.client
import org.http4s.client.RequestKey
import org.http4s.client.middleware.{Metrics, Retry, RetryPolicy, Logger => Http4sLogger}
import org.typelevel.log4cats.StructuredLogger
import org.typelevel.log4cats.slf4j.Slf4jLogger
import scalacache.caffeine._

import java.net.{InetSocketAddress, SocketException}
import java.nio.file.Path
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

    createDependencies(applicationConfig.leoServiceAccountJsonFile).use {
      appDependencies => // TODO: azure fix, get this from config
        val cloudDependencies = appDependencies.cloudDependencies

        implicit val dbRef = appDependencies.dbReference
        implicit val openTelemetry = appDependencies.openTelemetryMetrics

        val dateAccessedUpdater =
          new DateAccessedUpdater(dateAccessUpdaterConfig, appDependencies.dateAccessedUpdaterQueue)

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

        val googleOnlyServices = cloudDependencies match {
          case CloudDependencies.Google(googleStorageService,
                                        googleComputeService,
                                        _,
                                        googleResourceService,
                                        _,
                                        _,
                                        _,
                                        _,
                                        googleDiskService,
                                        googleProjectDAO,
                                        _,
                                        _
              ) =>
            val runtimeService = RuntimeService(
              runtimeServiceConfig,
              ConfigReader.appConfig.persistentDisk,
              appDependencies.authProvider,
              appDependencies.serviceAccountProvider,
              appDependencies.dockerDAO,
              googleStorageService,
              googleComputeService,
              appDependencies.publisherQueue
            )
            val diskService = new DiskServiceInterp[IO](
              ConfigReader.appConfig.persistentDisk,
              appDependencies.authProvider,
              appDependencies.serviceAccountProvider,
              appDependencies.publisherQueue,
              googleDiskService,
              googleProjectDAO
            )
            val proxyService = new ProxyService(
              appDependencies.sslContext,
              proxyConfig,
              appDependencies.jupyterDAO,
              appDependencies.runtimeDnsCache,
              appDependencies.kubernetesDnsCache,
              appDependencies.authProvider,
              appDependencies.dateAccessedUpdaterQueue,
              appDependencies.googleOauth2DAO,
              appDependencies.proxyResolver,
              appDependencies.samDAO,
              appDependencies.googleTokenCache,
              appDependencies.samResourceCache
            )
            Some(
              GCPModeSpecificServices(runtimeService,
                                      diskService,
                                      proxyService,
                                      googleResourceService,
                                      googleComputeService
              )
            )
          case _: CloudDependencies.Azure =>
            None
        }

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

        val httpRoutes = new HttpRoutes(
          appDependencies.openIDConnectConfiguration,
          statusService,
          diskV2Service,
          leoKubernetesService,
          azureService,
          adminService,
          googleOnlyServices,
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
              cloudDependencies match {
                case x: CloudDependencies.Google =>
                  x.dataprocInterp.setupDataprocImageGoogleGroup
                case _: CloudDependencies.Azure => IO.unit
              }

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

          val autopauseMonitorProcess = AutopauseMonitor.process(
            autoFreezeConfig,
            appDependencies.jupyterDAO,
            appDependencies.publisherQueue
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

          val backLeoOnlyProcesses =
            cloudDependencies match {
              case CloudDependencies.Google(_,
                                            googleComputeService,
                                            _,
                                            _,
                                            _,
                                            messageSubscriber,
                                            cryptoMiningUserPublisher,
                                            gkeAlg,
                                            _,
                                            _,
                                            _,
                                            nonLeoMessageGoogleSubscriber
                  ) =>
                val monitorAtBoot =
                  new MonitorAtBoot[IO](
                    appDependencies.publisherQueue,
                    googleComputeService,
                    appDependencies.samDAO,
                    appDependencies.wsmDAO
                  )

                val nonLeoMessageSubscriber =
                  new NonLeoMessageSubscriber[IO](
                    NonLeoMessageSubscriberConfig(gceConfig.userDiskDeviceName),
                    gkeAlg,
                    googleComputeService,
                    appDependencies.samDAO,
                    appDependencies.authProvider,
                    nonLeoMessageGoogleSubscriber,
                    cryptoMiningUserPublisher,
                    appDependencies.asyncTasksQueue
                  )

                List(
                  nonLeoMessageSubscriber.process,
                  Stream.eval(nonLeoMessageGoogleSubscriber.start),
                  asyncTasks.process,
                  appDependencies.pubsubSubscriber.process(messageSubscriber),
                  Stream.eval(messageSubscriber.start),
                  monitorAtBoot.process, // checks database to see if there's on-going runtime status transition
                  autopauseMonitorProcess, // check database to autopause runtimes periodically
                  metricsMonitor.process // checks database and collects metrics about active runtimes and apps
                )

              case azureDeps: CloudDependencies.Azure =>
                List(
                  asyncTasks.process,
                  appDependencies.pubsubSubscriber.process(azureDeps.messageSubscriber),
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
            appDependencies.cloudDependencies.leoPublisher.process, // start the publisher queue .dequeue
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

  private def createDependencies(
    pathToCredentialJson: Option[Path]
  )(implicit
    logger: StructuredLogger[IO],
    ec: ExecutionContext,
    as: ActorSystem,
    asyncIO: Async[IO]
  ): Resource[IO, AppDependencies] =
    for {
      semaphore <- Resource.eval(Semaphore[IO](applicationConfig.concurrency))
      // This is for sending custom metrics to stackdriver. all custom metrics starts with `OpenCensus/leonardo/`.
      // Typing in `leonardo` in metrics explorer will show all leonardo custom metrics.
      // As best practice, we should have all related metrics under same prefix separated by `/`
      implicit0(openTelemetry: OpenTelemetryMetrics[IO]) <- OpenTelemetryMetrics
        .resource[IO](applicationConfig.applicationName, prometheusConfig.endpointPort)

      // Set up database reference
      concurrentDbAccessPermits <- Resource.eval(Semaphore[IO](dbConcurrency))
      implicit0(dbRef: DbReference[IO]) <- DbReference.init(liquibaseConfig, concurrentDbAccessPermits)

      // Set up DNS caches
      hostToIpMapping <- Resource.eval(Ref.of(Map.empty[String, IP]))
      proxyResolver <- Dispatcher.parallel[IO].map(d => ProxyResolver(hostToIpMapping, d))

      underlyingRuntimeDnsCache = buildCache[RuntimeDnsCacheKey, scalacache.Entry[HostStatus]](
        runtimeDnsCacheConfig.cacheMaxSize,
        runtimeDnsCacheConfig.cacheExpiryTime
      )
      runtimeDnsCaffeineCache <- Resource.make(
        IO.delay(CaffeineCache[IO, RuntimeDnsCacheKey, HostStatus](underlyingRuntimeDnsCache))
      )(_.close)
      runtimeDnsCache = new RuntimeDnsCache(proxyConfig, dbRef, hostToIpMapping, runtimeDnsCaffeineCache)
      underlyingKubernetesDnsCache = buildCache[KubernetesDnsCacheKey, scalacache.Entry[HostStatus]](
        kubernetesDnsCacheConfig.cacheMaxSize,
        kubernetesDnsCacheConfig.cacheExpiryTime
      )

      kubernetesDnsCaffineCache <- Resource.make(
        IO.delay(CaffeineCache[IO, KubernetesDnsCacheKey, HostStatus](underlyingKubernetesDnsCache))
      )(_.close)
      kubernetesDnsCache = new KubernetesDnsCache(proxyConfig, dbRef, hostToIpMapping, kubernetesDnsCaffineCache)

      // Set up SSL context and http clients
      sslContext <- Resource.eval[IO, SSLContext](SslContextReader.getSSLContext())
      underlyingPetKeyCache = buildCache[UserEmailAndProject, scalacache.Entry[Option[io.circe.Json]]](
        httpSamDaoConfig.petCacheMaxSize,
        httpSamDaoConfig.petCacheExpiryTime
      )
      petKeyCache <- Resource.make(
        IO.delay(CaffeineCache[IO, UserEmailAndProject, Option[io.circe.Json]](underlyingPetKeyCache))
      )(_.close)

      samDao <- buildHttpClient(sslContext, proxyResolver.resolveHttp4s, Some("leo_sam_client"), true).map(client =>
        HttpSamDAO[IO](client, httpSamDaoConfig, petKeyCache)
      )
      cromwellDao <- buildHttpClient(sslContext, proxyResolver.resolveHttp4s, Some("leo_cromwell_client"), false).map(
        client => new HttpCromwellDAO[IO](client)
      )
      cbasDao <- buildHttpClient(sslContext, proxyResolver.resolveHttp4s, Some("leo_cbas_client"), false).map(client =>
        new HttpCbasDAO[IO](client)
      )
      wdsDao <- buildHttpClient(sslContext, proxyResolver.resolveHttp4s, Some("leo_wds_client"), false).map(client =>
        new HttpWdsDAO[IO](client)
      )
      hailBatchDao <- buildHttpClient(sslContext, proxyResolver.resolveHttp4s, Some("leo_hail_batch_client"), false)
        .map(client => new HttpHailBatchDAO[IO](client))
      listenerDao <- buildHttpClient(sslContext, proxyResolver.resolveHttp4s, Some("leo_listener_client"), false).map(
        client => new HttpListenerDAO[IO](client)
      )
      jupyterDao <- buildHttpClient(sslContext, proxyResolver.resolveHttp4s, Some("leo_jupyter_client"), false).map(
        client => new HttpJupyterDAO[IO](runtimeDnsCache, client, samDao)
      )
      welderDao <- buildHttpClient(sslContext, proxyResolver.resolveHttp4s, Some("leo_welder_client"), false).map(
        client => new HttpWelderDAO[IO](runtimeDnsCache, client, samDao)
      )
      rstudioDAO <- buildHttpClient(sslContext, proxyResolver.resolveHttp4s, Some("leo_rstudio_client"), false).map(
        client => new HttpRStudioDAO(runtimeDnsCache, client)
      )
      appDAO <- buildHttpClient(sslContext, proxyResolver.resolveHttp4s, Some("leo_app_client"), false).map(client =>
        new HttpAppDAO(kubernetesDnsCache, client)
      )
      dockerDao <- buildHttpClient(sslContext, proxyResolver.resolveHttp4s, None, true).map(client =>
        HttpDockerDAO[IO](client)
      )
      appDescriptorDAO <- buildHttpClient(sslContext, proxyResolver.resolveHttp4s, None, true).map(client =>
        new HttpAppDescriptorDAO(client)
      )
      wsmDao <- buildHttpClient(sslContext, proxyResolver.resolveHttp4s, Some("leo_wsm_client"), true)
        .map(client => new HttpWsmDao[IO](client, ConfigReader.appConfig.azure.wsm))
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
      authCache <- Resource.make(IO.delay(CaffeineCache[IO, AuthCacheKey, Boolean](underlyingAuthCache)))(s => s.close)
      authProvider = new SamAuthProvider(samDao, samAuthConfig, serviceAccountProvider, authCache)

      underlyingNodepoolLockCache = buildCache[KubernetesClusterId, scalacache.Entry[Semaphore[IO]]](
        gkeClusterConfig.nodepoolLockCacheMaxSize,
        gkeClusterConfig.nodepoolLockCacheExpiryTime
      )
      nodepoolLockCache <- Resource.make(
        IO.delay(CaffeineCache[IO, KubernetesClusterId, Semaphore[IO]](underlyingNodepoolLockCache))
      )(_.close)
      nodepoolLock = KeyLock[IO, KubernetesClusterId](nodepoolLockCache)

      dataAccessedUpdater <- Resource.eval(
        Queue.bounded[IO, UpdateDateAccessedMessage](dateAccessUpdaterConfig.queueSize)
      )

      asyncTasksQueue <- Resource.eval(Queue.bounded[IO, Task[IO]](asyncTaskProcessorConfig.queueBound))

      // Set up k8s and helm clients
      underlyingKubeClientCache = buildCache[KubernetesClusterId, scalacache.Entry[ApiClient]](
        200,
        2 hours
      )
      kubeCache <- Resource.make(
        IO.delay(CaffeineCache[IO, KubernetesClusterId, ApiClient](underlyingKubeClientCache))
      )(
        _.close
      )

      // Use a low concurrency for helm because it can generate very chatty network traffic
      // (especially for Galaxy) and cause issues at high concurrency.
      helmConcurrency <- Resource.eval(Semaphore[IO](20L))
      helmClient = new HelmInterpreter[IO](helmConcurrency)

      underlyingGoogleTokenCache = buildCache[String, scalacache.Entry[(UserInfo, Instant)]](
        proxyConfig.tokenCacheMaxSize,
        proxyConfig.tokenCacheExpiryTime
      )
      googleTokenCache <- Resource.make(
        IO.delay(CaffeineCache[IO, String, (UserInfo, Instant)](underlyingGoogleTokenCache))
      )(_.close)

      underlyingSamResourceCache = buildCache[SamResourceCacheKey,
                                              scalacache.Entry[(Option[String], Option[AppAccessScope])]
      ](
        proxyConfig.internalIdCacheMaxSize,
        proxyConfig.internalIdCacheExpiryTime
      )
      samResourceCache <- Resource.make(
        IO.delay(
          CaffeineCache[IO, SamResourceCacheKey, (Option[String], Option[AppAccessScope])](underlyingSamResourceCache)
        )
      )(s => s.close)

      underlyingOperationFutureCache = buildCache[Long, scalacache.Entry[OperationFuture[Operation, Operation]]](
        500,
        5 minutes
      )
      operationFutureCache <- Resource.make(
        IO.delay(CaffeineCache[IO, Long, OperationFuture[Operation, Operation]](underlyingOperationFutureCache))
      )(_.close)

      oidcConfig <- Resource.eval(
        OpenIDConnectConfiguration[IO](
          ConfigReader.appConfig.oidc.authorityEndpoint.renderString,
          ConfigReader.appConfig.oidc.clientId,
          oidcClientSecret = ConfigReader.appConfig.oidc.clientSecret,
          extraGoogleClientId = Some(ConfigReader.appConfig.oidc.legacyGoogleClientId),
          extraAuthParams = Some("prompt=login")
        )
      )

      recordMetricsProcesses = List(
        CacheMetrics[IO]("authCache").processWithUnderlyingCache(underlyingAuthCache),
        CacheMetrics[IO]("petTokenCache")
          .processWithUnderlyingCache(underlyingPetKeyCache),
        CacheMetrics[IO]("googleTokenCache")
          .processWithUnderlyingCache(underlyingGoogleTokenCache),
        CacheMetrics[IO]("samResourceCache")
          .processWithUnderlyingCache(underlyingSamResourceCache),
        CacheMetrics[IO]("runtimeDnsCache")
          .processWithUnderlyingCache(underlyingRuntimeDnsCache),
        CacheMetrics[IO]("kubernetesDnsCache")
          .processWithUnderlyingCache(underlyingKubernetesDnsCache),
        CacheMetrics[IO]("kubernetesApiClient")
          .processWithUnderlyingCache(underlyingKubeClientCache)
      )

      // Set up PubSub queues
      publisherQueue <- Resource.eval(Queue.bounded[IO, LeoPubsubMessage](pubsubConfig.queueSize))

      // Set up GCP credentials
      cloudDependencies <- pathToCredentialJson match {
        case Some(credentialPath) =>
          val credPathString = credentialPath.toString
          for {
            credential <- credentialResource[IO](credPathString)
            scopedCredential = credential.createScoped(Seq("https://www.googleapis.com/auth/compute").asJava)
            kubernetesScopedCredential = credential.createScoped(Seq(ContainerScopes.CLOUD_PLATFORM).asJava)

            credentialJson <- Resource.eval(
              readFileToString[IO](credentialPath)
            )
            json = Json(credentialJson)
            jsonWithServiceAccountUser = Json(credentialJson, Option(googleGroupsConfig.googleAdminEmail))

            // Set up Google DAOs
            googleProjectDAO = new HttpGoogleProjectDAO(applicationConfig.applicationName,
                                                        json,
                                                        workbenchMetricsBaseName
            )
            googleIamDAO = new HttpGoogleIamDAO(applicationConfig.applicationName, json, workbenchMetricsBaseName)
            googleDirectoryDAO = new HttpGoogleDirectoryDAO(applicationConfig.applicationName,
                                                            jsonWithServiceAccountUser,
                                                            workbenchMetricsBaseName
            )
            googleResourceService <-
              GoogleResourceService.resource[IO](credentialPath, semaphore)
            googleStorage <- GoogleStorageService.resource[IO](credPathString, Some(semaphore))
            googlePublisher <- GooglePublisher.cloudPublisherResource[IO](publisherConfig(credentialPath))
            cryptoMiningUserPublisher <- GooglePublisher.cloudPublisherResource[IO](cryptominingTopicPublisherConfig(credentialPath))
            gkeService <- GKEService.resource(credentialPath, semaphore)

            // Retry 400 responses from Google, as those can occur when resources aren't ready yet
            // (e.g. if the subnet isn't ready when creating an instance).
            googleComputeRetryPolicy = RetryPredicates.retryConfigWithPredicates(
              RetryPredicates.standardGoogleRetryPredicate,
              RetryPredicates.whenStatusCode(400)
            )

            kubeService <- org.broadinstitute.dsde.workbench.google2.KubernetesService
              .resource(credentialPath, gkeService, kubeCache)
            googleComputeService <-
              GoogleComputeService.fromCredential(
                scopedCredential,
                semaphore,
                googleComputeRetryPolicy
              )

            dataprocService <-
              GoogleDataprocService
                .resource(
                  googleComputeService,
                  credPathString,
                  semaphore,
                  dataprocConfig.supportedRegions,
                  googleDataprocRetryPolicy
                )

            _ <- OpenTelemetryMetrics.registerTracing[IO](credentialPath)

            googleDiskService <- GoogleDiskService.resource(credPathString, semaphore)

            subscriberQueue <- Resource.eval(Queue.bounded[IO, ReceivedMessage[LeoPubsubMessage]](pubsubConfig.queueSize))
            subscriber <- GoogleSubscriber.resource(subscriberConfig(credentialPath), subscriberQueue)

            nonLeoMessageSubscriberQueue <- Resource.eval(
              Queue.bounded[IO, ReceivedMessage[NonLeoMessage]](pubsubConfig.queueSize)
            )
            nonLeoMessageSubscriber <- GoogleSubscriber.resource(nonLeoMessageSubscriberConfig(credentialPath),
                                                                 nonLeoMessageSubscriberQueue
            )
          } yield {
            val bucketHelperConfig = BucketHelperConfig(
              imageConfig,
              welderConfig,
              proxyConfig,
              securityFilesConfig
            )
            val bucketHelper = new BucketHelper[IO](bucketHelperConfig, googleStorage, serviceAccountProvider)
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
              googleComputeService,
              googleDiskService,
              welderDao
            )

            val runtimeInstances = new RuntimeInstances(dataprocInterp, gceInterp)

            implicit val clusterToolToToolDao = ToolDAO.clusterToolToToolDao(jupyterDao, welderDao, rstudioDAO)
            val gceRuntimeMonitor = new GceRuntimeMonitor[IO](
              gceMonitorConfig,
              googleComputeService,
              authProvider,
              googleStorage,
              googleDiskService,
              publisherQueue,
              gceInterp
            )

            val dataprocRuntimeMonitor = new DataprocRuntimeMonitor[IO](
              dataprocMonitorConfig,
              googleComputeService,
              authProvider,
              googleStorage,
              googleDiskService,
              dataprocInterp,
              dataprocService
            )

            val cloudServiceRuntimeMonitor =
              new CloudServiceRuntimeMonitor(gceRuntimeMonitor, dataprocRuntimeMonitor)

            val gkeAlg = new GKEInterpreter[IO](
              gkeInterpConfig,
              bucketHelper,
              vpcInterp,
              gkeService,
              kubeService,
              helmClient,
              appDAO,
              kubernetesScopedCredential,
              googleIamDAO,
              googleDiskService,
              appDescriptorDAO,
              nodepoolLock,
              googleResourceService,
              googleComputeService
            )

            CloudDependencies.Google(
              googleStorage,
              googleComputeService,
              dataprocInterp,
              googleResourceService,
              leoPublisher = new LeoPublisher(publisherQueue, googlePublisher),
              subscriber,
              cryptoMiningUserPublisher,
              gkeAlg,
              googleDiskService,
              googleProjectDAO,
              Some(
                GCPModeSpecificDependencies(googleDiskService, gkeAlg, runtimeInstances, cloudServiceRuntimeMonitor)
              ),
              nonLeoMessageSubscriber
            )
          }
          //This where Azure Dependencies are created when hosting Leonardo on Azure.
        case None =>
          val pubConfig = AzureServiceBusPublisherConfig(
            azurePubSubConfig.topic,
            azurePubSubConfig.connectionString,
            azurePubSubConfig.namespace
          )

          val subConfig = AzureServiceBusSubscriberConfig(
            azurePubSubConfig.topic,
            azurePubSubConfig.subscription,
            azurePubSubConfig.connectionString,
            azurePubSubConfig.namespace
          )

          for{
            subscriberQueue <- Resource.eval(Queue.bounded[IO, ReceivedMessage[LeoPubsubMessage]](azurePubSubConfig.queueSize))

            cloudSubscriber <- AzureSubscriberInterpreter.subscriber(subConfig, subscriberQueue)

            publisher <- AzurePublisherInterpreter.publisher(pubConfig)

            leoPublisher = new LeoPublisher(publisherQueue, publisher)

          }yield{
            CloudDependencies.Azure(
              leoPublisher,
              cloudSubscriber,
              gcpModeSpecificDependencies = None
            )
          }
      }
    } yield {
      val kubeAlg = new KubernetesInterpreter[IO](
        azureContainerService
      )

      val cromwellAppInstall = new CromwellAppInstall[IO](

        ConfigReader.appConfig.azure.coaAppConfig,
        ConfigReader.appConfig.drs,
        samDao,
        cromwellDao,
        cbasDao,
        azureBatchService,
        azureApplicationInsightsService
      )
      val cromwellRunnerAppInstall =
        new CromwellRunnerAppInstall[IO](ConfigReader.appConfig.azure.cromwellRunnerAppConfig,
                                         ConfigReader.appConfig.drs,
                                         samDao,
                                         cromwellDao,
                                         azureBatchService,
                                         azureApplicationInsightsService
        )
      val hailBatchAppInstall =
        new HailBatchAppInstall[IO](ConfigReader.appConfig.azure.hailBatchAppConfig, hailBatchDao)
      val wdsAppInstall = new WdsAppInstall[IO](ConfigReader.appConfig.azure.wdsAppConfig,
                                                ConfigReader.appConfig.azure.tdr,
                                                samDao,
                                                wdsDao,
                                                azureApplicationInsightsService
      )
      val workflowsAppInstall =
        new WorkflowsAppInstall[IO](
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

      val aksAlg = new AKSInterpreter[IO](
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

      val azureAlg = new AzurePubsubHandlerInterp[IO](
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

      // TODO: need fix for Azure
      val pubsubSubscriber = new LeoPubsubMessageSubscriber[IO](
        leoPubsubMessageSubscriberConfig,
        asyncTasksQueue,
        authProvider,
        azureAlg,
        operationFutureCache,
        cloudDependencies.gcpModeSpecificDependencies
      )

      AppDependencies(
        sslContext,
        dbRef,
        runtimeDnsCache,
        cloudDependencies,
        kubernetesDnsCache,
        googleOauth2DAO,
        samDao,
        dockerDao,
        jupyterDao,
        rstudioDAO,
        welderDao,
        wsmDao,
        serviceAccountProvider,
        authProvider,
        publisherQueue,
        dataAccessedUpdater,
        asyncTasksQueue,
        nodepoolLock,
        proxyResolver,
        recordMetricsProcesses,
        googleTokenCache,
        samResourceCache,
        pubsubSubscriber,
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
        azureContainerService,
        openTelemetry
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

  private def buildHttpClient(
    sslContext: SSLContext,
    dnsResolver: RequestKey => Either[Throwable, InetSocketAddress],
    metricsPrefix: Option[String],
    withRetry: Boolean
  )(implicit logger: StructuredLogger[IO]): Resource[IO, org.http4s.client.Client[IO]] = {
    // Retry all SocketExceptions to deal with pooled HTTP connections getting closed.
    // See https://broadworkbench.atlassian.net/browse/IA-4069.
    val retryPolicy = RetryPolicy[IO](
      RetryPolicy.exponentialBackoff(30 seconds, 5),
      (req, result) =>
        result match {
          case Left(e) if e.isInstanceOf[SocketException] => true
          case _                                          => RetryPolicy.defaultRetriable(req, result)
        }
    )

    for {
      httpClient <- client
        .BlazeClientBuilder[IO]
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
      httpClientWithLogging = Http4sLogger[IO](logHeaders = true,
                                               logBody = false,
                                               logAction = Some(s => logAction[IO](s))
      )(
        httpClient
      )
      clientWithRetry = if (withRetry) Retry(retryPolicy)(httpClientWithLogging) else httpClientWithLogging
      finalClient <- metricsPrefix match {
        case None => Resource.pure[IO, org.http4s.client.Client[IO]](clientWithRetry)
        case Some(prefix) =>
          val classifierFunc = (r: Request[IO]) => Some(r.method.toString.toLowerCase)
          for {
            metricsOps <- org.http4s.metrics.prometheus.Prometheus
              .metricsOps[IO](io.prometheus.client.CollectorRegistry.defaultRegistry, prefix)
            meteredClient = Metrics[IO](
              metricsOps,
              classifierFunc
            )(clientWithRetry)
          } yield meteredClient
      }
    } yield finalClient
  }

  override def run(args: List[String]): IO[ExitCode] = startup().as(ExitCode.Success)
}

sealed trait CloudDependencies extends Product with Serializable {
  def leoPublisher: LeoPublisher[IO]
  def gcpModeSpecificDependencies: Option[GCPModeSpecificDependencies[IO]]
}
object CloudDependencies {
  final case class Google(
    googleStorageService: GoogleStorageService[IO],
    googleComputeService: GoogleComputeService[IO],
    dataprocInterp: DataprocInterpreter[IO],
    googleResourceService: GoogleResourceService[IO],
    leoPublisher: LeoPublisher[IO], // TODO: Jesus is working on fix this
    messageSubscriber: CloudSubscriber[IO, LeoPubsubMessage],
    cryptoMiningUserPublisher: CloudPublisher[IO],
    gkeAlg: GKEAlgebra[IO],
    googleDiskService: GoogleDiskService[IO],
    googleProjectDAO: GoogleProjectDAO,
    gcpModeSpecificDependencies: Option[GCPModeSpecificDependencies[IO]],
    nonLeoMessageGoogleSubscriber: CloudSubscriber[IO, NonLeoMessage]
  ) extends CloudDependencies

  final case class Azure(
    leoPublisher: LeoPublisher[IO],
    messageSubscriber: CloudSubscriber[IO, LeoPubsubMessage],
    gcpModeSpecificDependencies: Option[GCPModeSpecificDependencies[IO]]
  ) extends CloudDependencies
}

final case class AppDependencies(
  sslContext: SSLContext,
  dbReference: DbReference[IO],
  runtimeDnsCache: RuntimeDnsCache[IO],
  cloudDependencies: CloudDependencies,
  kubernetesDnsCache: KubernetesDnsCache[IO],
  googleOauth2DAO: GoogleOAuth2Service[IO],
  samDAO: HttpSamDAO[IO],
  dockerDAO: HttpDockerDAO[IO],
  jupyterDAO: HttpJupyterDAO[IO],
  rstudioDAO: HttpRStudioDAO[IO],
  welderDAO: HttpWelderDAO[IO],
  wsmDAO: HttpWsmDao[IO],
  serviceAccountProvider: ServiceAccountProvider[IO],
  authProvider: SamAuthProvider[IO],
  publisherQueue: Queue[IO, LeoPubsubMessage],
  dateAccessedUpdaterQueue: Queue[IO, UpdateDateAccessedMessage],
  asyncTasksQueue: Queue[IO, Task[IO]],
  nodepoolLock: KeyLock[IO, KubernetesClusterId],
  proxyResolver: ProxyResolver[IO],
  recordCacheMetrics: List[Stream[IO, Unit]],
  googleTokenCache: scalacache.Cache[IO, String, (UserInfo, Instant)],
  samResourceCache: scalacache.Cache[IO, SamResourceCacheKey, (Option[String], Option[AppAccessScope])],
  pubsubSubscriber: LeoPubsubMessageSubscriber[IO],
  openIDConnectConfiguration: OpenIDConnectConfiguration,
  aksInterp: AKSAlgebra[IO],
  appDAO: AppDAO[IO],
  wdsDAO: WdsDAO[IO],
  cbasDAO: CbasDAO[IO],
  cromwellDAO: CromwellDAO[IO],
  hailBatchDAO: HailBatchDAO[IO],
  listenerDAO: ListenerDAO[IO],
  wsmClientProvider: HttpWsmClientProvider[IO],
  kubeAlg: KubernetesAlgebra[IO],
  azureContainerService: AzureContainerService[IO],
  openTelemetryMetrics: OpenTelemetryMetrics[IO]
)
