package org.broadinstitute.dsde.workbench.leonardo
package http

import java.nio.file.Paths

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import cats.Parallel
import cats.effect.concurrent.Semaphore
import cats.effect.{Blocker, ConcurrentEffect, ContextShift, ExitCode, IO, IOApp, Resource, Timer}
import cats.implicits._
import fs2.Stream
import com.typesafe.sslconfig.akka.util.AkkaLoggerFactory
import com.typesafe.sslconfig.ssl.{
  ConfigSSLContextBuilder,
  DefaultKeyManagerFactoryWrapper,
  DefaultTrustManagerFactoryWrapper,
  SSLConfigFactory
}
import fs2.concurrent.InspectableQueue
import io.chrisdavenport.log4cats.StructuredLogger
import io.chrisdavenport.log4cats.slf4j.Slf4jLogger
import javax.net.ssl.SSLContext
import org.broadinstitute.dsde.workbench.google.GoogleCredentialModes.{Json, Token}
import org.broadinstitute.dsde.workbench.google2.{GoogleDataprocService, GoogleDiskService}
import org.broadinstitute.dsde.workbench.leonardo.AsyncTaskProcessor.Task
import org.broadinstitute.dsde.workbench.google.{
  GoogleStorageDAO,
  HttpGoogleDirectoryDAO,
  HttpGoogleIamDAO,
  HttpGoogleProjectDAO,
  HttpGoogleStorageDAO
}
import org.broadinstitute.dsde.workbench.google2.{
  Event,
  GoogleComputeService,
  GooglePublisher,
  GoogleStorageService,
  GoogleSubscriber
}
import org.broadinstitute.dsde.workbench.leonardo.auth.sam.{PetClusterServiceAccountProvider, SamAuthProvider}
import org.broadinstitute.dsde.workbench.leonardo.config.Config._
import org.broadinstitute.dsde.workbench.leonardo.config.LeoExecutionModeConfig
import org.broadinstitute.dsde.workbench.leonardo.dao._
import org.broadinstitute.dsde.workbench.leonardo.dao.google.HttpGoogleDataprocDAO
import org.broadinstitute.dsde.workbench.leonardo.db.DbReference
import org.broadinstitute.dsde.workbench.leonardo.dns.ClusterDnsCache
import org.broadinstitute.dsde.workbench.leonardo.http.api.{HttpRoutes, StandardUserInfoDirectives}
import org.broadinstitute.dsde.workbench.leonardo.http.service.{DiskServiceInterp, LeoKubernetesServiceInterp, _}
import org.broadinstitute.dsde.workbench.leonardo.model.{LeoAuthProvider, ServiceAccountProvider}
import org.broadinstitute.dsde.workbench.leonardo.monitor.LeoPubsubCodec._
import org.broadinstitute.dsde.workbench.leonardo.monitor._
import org.broadinstitute.dsde.workbench.leonardo.util._
import org.broadinstitute.dsde.workbench.openTelemetry.OpenTelemetryMetrics
import org.broadinstitute.dsde.workbench.util.ExecutionContexts
import org.http4s.client.blaze
import org.http4s.client.middleware.{Retry, RetryPolicy, Logger => Http4sLogger}

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._

object Boot extends IOApp {
  val workbenchMetricsBaseName = "google"

  private def startup(): IO[Unit] = {
    // We need an ActorSystem to host our application in
    implicit val system = ActorSystem(applicationConfig.applicationName)
    import system.dispatcher

    implicit val logger = Slf4jLogger.getLogger[IO]

    createDependencies[IO](applicationConfig.leoServiceAccountJsonFile.toString).use { appDependencies =>
      implicit val openTelemetry = appDependencies.openTelemetryMetrics
      implicit val dbRef = appDependencies.dbReference

      val bucketHelperConfig = BucketHelperConfig(
        imageConfig,
        welderConfig,
        proxyConfig,
        clusterFilesConfig,
        clusterResourcesConfig
      )
      val bucketHelper = new BucketHelper(bucketHelperConfig,
                                          appDependencies.google2StorageDao,
                                          appDependencies.serviceAccountProvider,
                                          appDependencies.blocker)
      val vpcInterp =
        new VPCInterpreter(vpcInterpreterConfig, appDependencies.googleProjectDAO, appDependencies.googleComputeService)

      val dataprocInterp = new DataprocInterpreter(dataprocInterpreterConfig,
                                                   bucketHelper,
                                                   vpcInterp,
                                                   appDependencies.googleDataprocDAO,
                                                   appDependencies.googleComputeService,
                                                   appDependencies.googleDiskService,
                                                   appDependencies.googleDirectoryDAO,
                                                   appDependencies.googleIamDAO,
                                                   appDependencies.googleProjectDAO,
                                                   appDependencies.welderDAO,
                                                   appDependencies.blocker)

      val gceInterp = new GceInterpreter(gceInterpreterConfig,
                                         bucketHelper,
                                         vpcInterp,
                                         appDependencies.googleComputeService,
                                         appDependencies.googleDiskService,
                                         appDependencies.welderDAO,
                                         appDependencies.blocker)
      implicit val runtimeInstances = new RuntimeInstances(dataprocInterp, gceInterp)

      val leonardoService = new LeonardoService(dataprocConfig,
                                                imageConfig,
                                                appDependencies.welderDAO,
                                                proxyConfig,
                                                swaggerConfig,
                                                autoFreezeConfig,
                                                zombieRuntimeMonitorConfig,
                                                welderConfig,
                                                appDependencies.petGoogleStorageDAO,
                                                appDependencies.authProvider,
                                                appDependencies.serviceAccountProvider,
                                                bucketHelper,
                                                appDependencies.dockerDAO,
                                                appDependencies.publisherQueue)
      val dateAccessedUpdater =
        new DateAccessedUpdater(dateAccessUpdaterConfig, appDependencies.dateAccessedUpdaterQueue)
      val proxyService = new ProxyService(proxyConfig,
                                          appDependencies.googleDataprocDAO,
                                          appDependencies.clusterDnsCache,
                                          appDependencies.authProvider,
                                          appDependencies.dateAccessedUpdaterQueue,
                                          appDependencies.blocker)
      val statusService = new StatusService(appDependencies.googleDataprocDAO,
                                            appDependencies.samDAO,
                                            appDependencies.dbReference,
                                            applicationConfig)
      val runtimeServiceConfig = RuntimeServiceConfig(
        proxyConfig.proxyUrlBase,
        imageConfig,
        autoFreezeConfig,
        zombieRuntimeMonitorConfig,
        dataprocConfig,
        gceConfig
      )
      val runtimeService = new RuntimeServiceInterp[IO](
        runtimeServiceConfig,
        persistentDiskConfig,
        appDependencies.authProvider,
        appDependencies.serviceAccountProvider,
        appDependencies.dockerDAO,
        appDependencies.google2StorageDao,
        appDependencies.publisherQueue
      )
      val diskService = new DiskServiceInterp[IO](
        persistentDiskConfig,
        appDependencies.authProvider,
        appDependencies.serviceAccountProvider,
        appDependencies.publisherQueue
      )

      val zombieClusterMonitor = ZombieRuntimeMonitor[IO](zombieRuntimeMonitorConfig, appDependencies.googleProjectDAO)

      val leoKubernetesService: LeoKubernetesServiceInterp[IO] =
        new LeoKubernetesServiceInterp(appDependencies.authProvider,
                                       appDependencies.serviceAccountProvider,
                                       leoKubernetesConfig,
                                       appDependencies.publisherQueue)

      val httpRoutes = new HttpRoutes(swaggerConfig,
                                      statusService,
                                      proxyService,
                                      leonardoService,
                                      runtimeService,
                                      diskService,
                                      leoKubernetesService,
                                      StandardUserInfoDirectives,
                                      contentSecurityPolicy)
      val httpServer = for {
        _ <- if (leoExecutionModeConfig == LeoExecutionModeConfig.BackLeoOnly) {
          dataprocInterp.setupDataprocImageGoogleGroup()
        } else IO.unit
        _ <- IO.fromFuture {
          IO {
            Http()
              .bindAndHandle(httpRoutes.route, "0.0.0.0", 8080)
              .onError {
                case t: Throwable =>
                  logger.error(t)("FATAL - failure starting http server").unsafeToFuture()
              }
          }
        }
      } yield ()

      val allStreams = {
        val backLeoOnlyProcesses = {
          implicit val clusterToolToToolDao =
            ToolDAO.clusterToolToToolDao(appDependencies.jupyterDAO,
                                         appDependencies.welderDAO,
                                         appDependencies.rStudioDAO)

          val gceRuntimeMonitor = new GceRuntimeMonitor[IO](
            gceMonitorConfig,
            appDependencies.googleComputeService,
            appDependencies.authProvider,
            appDependencies.google2StorageDao,
            gceInterp
          )

          val dataprocRuntimeMonitor =
            new DataprocRuntimeMonitor[IO](
              dataprocMonitorConfig,
              appDependencies.googleComputeService,
              appDependencies.authProvider,
              appDependencies.google2StorageDao,
              dataprocInterp,
              appDependencies.googleDataproc
            )

          implicit val cloudServiceRuntimeMonitor: RuntimeMonitor[IO, CloudService] =
            new CloudServiceRuntimeMonitor(gceRuntimeMonitor, dataprocRuntimeMonitor)

          val monitorAtBoot = new MonitorAtBoot[IO]()

          val googleDiskService = appDependencies.googleDiskService

          // only needed for backleo
          val asyncTasks = AsyncTaskProcessor(asyncTaskProcessorConfig, appDependencies.asyncTasksQueue)

          val pubsubSubscriber =
            new LeoPubsubMessageSubscriber[IO](leoPubsubMessageSubscriberConfig,
                                               appDependencies.subscriber,
                                               appDependencies.asyncTasksQueue,
                                               googleDiskService)

          val autopauseMonitor = AutopauseMonitor(
            autoFreezeConfig,
            appDependencies.jupyterDAO,
            appDependencies.publisherQueue
          )

          List(
            asyncTasks.process,
            pubsubSubscriber.process,
            Stream.eval(appDependencies.subscriber.start),
            zombieClusterMonitor.process, // mark runtimes that are no long active in google as zombie periodically
            monitorAtBoot.process, // checks database to see if there's on-going runtime status transition
            autopauseMonitor.process // check database to autopause runtimes periodically
          )
        }

        val frontLeoOnlyProcesses = List(dateAccessedUpdater.process) //We only need to update dateAccessed in front leo

        val extraProcesses = leoExecutionModeConfig match {
          case LeoExecutionModeConfig.BackLeoOnly  => backLeoOnlyProcesses
          case LeoExecutionModeConfig.FrontLeoOnly => frontLeoOnlyProcesses
          case LeoExecutionModeConfig.Combined     => backLeoOnlyProcesses ++ frontLeoOnlyProcesses
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

  private def createDependencies[F[_]: StructuredLogger: Parallel: ContextShift: Timer](
    pathToCredentialJson: String
  )(implicit ec: ExecutionContext, as: ActorSystem, F: ConcurrentEffect[F]): Resource[F, AppDependencies[F]] =
    for {
      blockingEc <- ExecutionContexts.cachedThreadPool[F]
      semaphore <- Resource.liftF(Semaphore[F](255L))
      blocker = Blocker.liftExecutionContext(blockingEc)
      storage <- GoogleStorageService.resource[F](pathToCredentialJson, blocker, Some(semaphore))
      retryPolicy = RetryPolicy[F](RetryPolicy.exponentialBackoff(30 seconds, 5))

      sslContext = getSSLContext()
      httpClientWithCustomSSL <- blaze.BlazeClientBuilder[F](blockingEc, Some(sslContext)).resource
      clientWithRetryWithCustomSSL = Retry(retryPolicy)(httpClientWithCustomSSL)
      clientWithRetryAndLogging = Http4sLogger[F](logHeaders = true, logBody = false)(clientWithRetryWithCustomSSL)

      samDao = HttpSamDAO[F](clientWithRetryAndLogging, httpSamDap2Config, blocker)
      concurrentDbAccessPermits <- Resource.liftF(Semaphore[F](dbConcurrency))
      implicit0(dbRef: DbReference[F]) <- DbReference.init(liquibaseConfig, concurrentDbAccessPermits, blocker)
      clusterDnsCache = new ClusterDnsCache(proxyConfig, dbRef, clusterDnsCacheConfig, blocker)
      // This is for sending custom metrics to stackdriver. all custom metrics starts with `OpenCensus/leonardo/`.
      // Typing in `leonardo` in metrics explorer will show all leonardo custom metrics.
      // As best practice, we should have all related metrics under same prefix separated by `/`
      implicit0(openTelemetry: OpenTelemetryMetrics[F]) <- OpenTelemetryMetrics
        .resource[F](applicationConfig.leoServiceAccountJsonFile, applicationConfig.applicationName, blocker)
      welderDao = new HttpWelderDAO[F](clusterDnsCache, clientWithRetryAndLogging)
      dockerDao = HttpDockerDAO[F](clientWithRetryAndLogging)
      jupyterDao = new HttpJupyterDAO[F](clusterDnsCache, clientWithRetryAndLogging)
      rstudioDAO = new HttpRStudioDAO(clusterDnsCache, clientWithRetryAndLogging)
      serviceAccountProvider = new PetClusterServiceAccountProvider(samDao)
      authProvider = new SamAuthProvider(samDao, samAuthConfig, serviceAccountProvider, blocker)

      credentialJson <- Resource.liftF(
        readFileToString(applicationConfig.leoServiceAccountJsonFile, blocker)
      )
      json = Json(credentialJson)
      jsonWithServiceAccountUser = Json(credentialJson, Option(googleGroupsConfig.googleAdminEmail))

      petGoogleStorageDAO = (token: String) =>
        new HttpGoogleStorageDAO(applicationConfig.applicationName, Token(() => token), workbenchMetricsBaseName)
      googleIamDAO = new HttpGoogleIamDAO(applicationConfig.applicationName, json, workbenchMetricsBaseName)
      googleDirectoryDAO = new HttpGoogleDirectoryDAO(applicationConfig.applicationName,
                                                      jsonWithServiceAccountUser,
                                                      workbenchMetricsBaseName)
      googleProjectDAO = new HttpGoogleProjectDAO(applicationConfig.applicationName, json, workbenchMetricsBaseName)
      gdDAO = new HttpGoogleDataprocDAO(applicationConfig.applicationName,
                                        json,
                                        workbenchMetricsBaseName,
                                        vpcConfig.networkTag,
                                        dataprocConfig.regionName,
                                        dataprocConfig.zoneName)

      googlePublisher <- GooglePublisher.resource[F](publisherConfig)

      publisherQueue <- Resource.liftF(InspectableQueue.bounded[F, LeoPubsubMessage](pubsubConfig.queueSize))
      dataAccessedUpdater <- Resource.liftF(
        InspectableQueue.bounded[F, UpdateDateAccessMessage](dateAccessUpdaterConfig.queueSize)
      )

      leoPublisher = new LeoPublisher(publisherQueue, googlePublisher)

      subscriberQueue <- Resource.liftF(InspectableQueue.bounded[F, Event[LeoPubsubMessage]](pubsubConfig.queueSize))
      subscriber <- GoogleSubscriber.resource(subscriberConfig, subscriberQueue)

      googleComputeService <- GoogleComputeService.resource(pathToCredentialJson, blocker, semaphore)
      dataprocService <- GoogleDataprocService.resource(
        pathToCredentialJson,
        blocker,
        semaphore,
        dataprocConfig.regionName
      )
      asyncTasksQueue <- Resource.liftF(InspectableQueue.bounded[F, Task[F]](asyncTaskProcessorConfig.queueBound))
      _ <- OpenTelemetryMetrics.registerTracing[F](Paths.get(pathToCredentialJson), blocker)
      googleDiskService <- GoogleDiskService.resource(pathToCredentialJson, blocker, semaphore)
    } yield AppDependencies(
      storage,
      dbRef,
      clusterDnsCache,
      petGoogleStorageDAO,
      googleComputeService,
      googleDiskService,
      googleProjectDAO,
      googleDirectoryDAO,
      googleIamDAO,
      gdDAO,
      dataprocService,
      samDao,
      welderDao,
      dockerDao,
      jupyterDao,
      rstudioDAO,
      serviceAccountProvider,
      authProvider,
      openTelemetry,
      blocker,
      semaphore,
      leoPublisher,
      publisherQueue,
      dataAccessedUpdater,
      subscriber,
      asyncTasksQueue
    )

  private def getSSLContext()(implicit as: ActorSystem): SSLContext = {
    val akkaOverrides = as.settings.config.getConfig("akka.ssl-config")
    val defaults = as.settings.config.getConfig("ssl-config")

    val sslConfigSettings = SSLConfigFactory.parse(akkaOverrides.withFallback(defaults))
    val keyManagerAlgorithm = new DefaultKeyManagerFactoryWrapper(sslConfigSettings.keyManagerConfig.algorithm)
    val trustManagerAlgorithm = new DefaultTrustManagerFactoryWrapper(sslConfigSettings.trustManagerConfig.algorithm)

    new ConfigSSLContextBuilder(new AkkaLoggerFactory(as),
                                sslConfigSettings,
                                keyManagerAlgorithm,
                                trustManagerAlgorithm).build()
  }

  override def run(args: List[String]): IO[ExitCode] = startup().as(ExitCode.Success)
}

final case class AppDependencies[F[_]](
  google2StorageDao: GoogleStorageService[F],
  dbReference: DbReference[F],
  clusterDnsCache: ClusterDnsCache[F],
  petGoogleStorageDAO: String => GoogleStorageDAO,
  googleComputeService: GoogleComputeService[F],
  googleDiskService: GoogleDiskService[F],
  googleProjectDAO: HttpGoogleProjectDAO,
  googleDirectoryDAO: HttpGoogleDirectoryDAO,
  googleIamDAO: HttpGoogleIamDAO,
  googleDataprocDAO: HttpGoogleDataprocDAO,
  googleDataproc: GoogleDataprocService[F],
  samDAO: HttpSamDAO[F],
  welderDAO: HttpWelderDAO[F],
  dockerDAO: HttpDockerDAO[F],
  jupyterDAO: HttpJupyterDAO[F],
  rStudioDAO: RStudioDAO[F],
  serviceAccountProvider: ServiceAccountProvider[F],
  authProvider: LeoAuthProvider[F],
  openTelemetryMetrics: OpenTelemetryMetrics[F],
  blocker: Blocker,
  semaphore: Semaphore[F],
  leoPublisher: LeoPublisher[F],
  publisherQueue: fs2.concurrent.InspectableQueue[F, LeoPubsubMessage],
  dateAccessedUpdaterQueue: fs2.concurrent.InspectableQueue[F, UpdateDateAccessMessage],
  subscriber: GoogleSubscriber[F, LeoPubsubMessage],
  asyncTasksQueue: InspectableQueue[F, Task[F]]
)
