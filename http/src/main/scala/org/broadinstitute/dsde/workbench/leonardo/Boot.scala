package org.broadinstitute.dsde.workbench.leonardo
package http

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import cats.effect.concurrent.Semaphore
import cats.effect.{Blocker, ConcurrentEffect, ContextShift, ExitCode, IO, IOApp, Resource, Timer}
import cats.implicits._
import com.google.protobuf.ByteString
import com.google.pubsub.v1.PubsubMessage
import com.typesafe.sslconfig.akka.util.AkkaLoggerFactory
import com.typesafe.sslconfig.ssl.{
  ConfigSSLContextBuilder,
  DefaultKeyManagerFactoryWrapper,
  DefaultTrustManagerFactoryWrapper,
  SSLConfigFactory
}
import fs2.concurrent.InspectableQueue
import fs2.{Pipe, Stream}
import io.chrisdavenport.log4cats.slf4j.Slf4jLogger
import io.chrisdavenport.log4cats.{Logger, StructuredLogger}
import io.circe.syntax._
import javax.net.ssl.SSLContext
import org.broadinstitute.dsde.workbench.google.GoogleCredentialModes.{Json, Token}
import org.broadinstitute.dsde.workbench.google.{
  GoogleStorageDAO,
  HttpGoogleDirectoryDAO,
  HttpGoogleIamDAO,
  HttpGoogleProjectDAO,
  HttpGoogleStorageDAO
}
import org.broadinstitute.dsde.workbench.google2.{
  Event,
  FirewallRuleName,
  GoogleComputeService,
  GooglePublisher,
  GoogleStorageService,
  GoogleSubscriber
}
import org.broadinstitute.dsde.workbench.leonardo.auth.sam.{PetClusterServiceAccountProvider, SamAuthProvider}
import org.broadinstitute.dsde.workbench.leonardo.config.Config._
import org.broadinstitute.dsde.workbench.leonardo.dao._
import org.broadinstitute.dsde.workbench.leonardo.dao.google.HttpGoogleDataprocDAO
import org.broadinstitute.dsde.workbench.leonardo.db.DbReference
import org.broadinstitute.dsde.workbench.leonardo.dns.ClusterDnsCache
import org.broadinstitute.dsde.workbench.leonardo.http.api.{LeoRoutes, StandardUserInfoDirectives}
import org.broadinstitute.dsde.workbench.leonardo.http.service._
import org.broadinstitute.dsde.workbench.leonardo.model.{LeoAuthProvider, ServiceAccountProvider}
import org.broadinstitute.dsde.workbench.leonardo.monitor.LeoPubsubCodec._
import org.broadinstitute.dsde.workbench.leonardo.monitor._
import org.broadinstitute.dsde.workbench.leonardo.util._
import org.broadinstitute.dsde.workbench.newrelic.NewRelicMetrics
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
    implicit def logger = Slf4jLogger.getLogger[IO]

    createDependencies(leoServiceAccountJsonFile).use { appDependencies =>
      implicit val metrics = appDependencies.metrics
      implicit val dbRef = appDependencies.dbReference

      val bucketHelperConfig = BucketHelperConfig(
        imageConfig,
        welderConfig,
        proxyConfig,
        clusterFilesConfig,
        clusterResourcesConfig
      )
      val bucketHelper = new BucketHelper(bucketHelperConfig,
                                          appDependencies.googleComputeService,
                                          appDependencies.googleStorageDAO,
                                          appDependencies.google2StorageDao,
                                          appDependencies.googleProjectDAO,
                                          appDependencies.serviceAccountProvider,
                                          appDependencies.blocker)

      val vpcHelperConfig = VPCHelperConfig(
        proxyConfig.projectVPCNetworkLabel,
        proxyConfig.projectVPCSubnetLabel,
        FirewallRuleName(proxyConfig.firewallRuleName),
        proxyConfig.proxyProtocol,
        proxyConfig.proxyPort,
        List(NetworkTag(proxyConfig.networkTag))
      )
      val vpcHelper =
        new VPCHelper(vpcHelperConfig, appDependencies.googleProjectDAO, appDependencies.googleComputeService)

      val clusterHelper = new ClusterHelper(dataprocConfig,
                                            imageConfig,
                                            googleGroupsConfig,
                                            proxyConfig,
                                            clusterResourcesConfig,
                                            clusterFilesConfig,
                                            monitorConfig,
                                            welderConfig,
                                            bucketHelper,
                                            vpcHelper,
                                            appDependencies.googleDataprocDAO,
                                            appDependencies.googleComputeService,
                                            appDependencies.googleDirectoryDAO,
                                            appDependencies.googleIamDAO,
                                            appDependencies.googleProjectDAO,
                                            appDependencies.welderDAO,
                                            appDependencies.blocker)

      val leonardoService = new LeonardoService(dataprocConfig,
                                                imageConfig,
                                                appDependencies.welderDAO,
                                                proxyConfig,
                                                swaggerConfig,
                                                autoFreezeConfig,
                                                welderConfig,
                                                appDependencies.petGoogleStorageDAO,
                                                appDependencies.authProvider,
                                                appDependencies.serviceAccountProvider,
                                                bucketHelper,
                                                clusterHelper,
                                                appDependencies.dockerDAO,
                                                appDependencies.publisherQueue)

      if (leoExecutionModeConfig.backLeo) {
        implicit def clusterToolToToolDao =
          ToolDAO.clusterToolToToolDao(appDependencies.jupyterDAO,
                                       appDependencies.welderDAO,
                                       appDependencies.rStudioDAO)
        system.actorOf(
          ClusterMonitorSupervisor.props(
            monitorConfig,
            dataprocConfig,
            imageConfig,
            clusterBucketConfig,
            appDependencies.googleDataprocDAO,
            appDependencies.googleComputeService,
            appDependencies.googleStorageDAO,
            appDependencies.google2StorageDao,
            appDependencies.authProvider,
            autoFreezeConfig,
            appDependencies.jupyterDAO,
            appDependencies.rStudioDAO,
            appDependencies.welderDAO,
            clusterHelper,
            appDependencies.publisherQueue
          )
        )
        system.actorOf(
          ZombieClusterMonitor.props(zombieClusterMonitorConfig,
                                     appDependencies.googleDataprocDAO,
                                     appDependencies.googleProjectDAO)
        )
        system.actorOf(
          ClusterToolMonitor.props(clusterToolMonitorConfig,
                                   appDependencies.googleDataprocDAO,
                                   appDependencies.googleProjectDAO,
                                   appDependencies.dbReference,
                                   appDependencies.metrics)
        )
      }
      val clusterDateAccessedActor =
        system.actorOf(ClusterDateAccessedActor.props(autoFreezeConfig, appDependencies.dbReference))
      val proxyService = new ProxyService(proxyConfig,
                                          appDependencies.googleDataprocDAO,
                                          appDependencies.clusterDnsCache,
                                          appDependencies.authProvider,
                                          clusterDateAccessedActor,
                                          appDependencies.blocker)
      val statusService = new StatusService(appDependencies.googleDataprocDAO,
                                            appDependencies.samDAO,
                                            appDependencies.dbReference,
                                            applicationConfig)
      val leoRoutes = new LeoRoutes(leonardoService, proxyService, statusService, swaggerConfig, contentSecurityPolicy)
      with StandardUserInfoDirectives

      val messageProcessorStream = if (leoExecutionModeConfig.backLeo) {
        val pubsubSubscriber: LeoPubsubMessageSubscriber[IO] =
          new LeoPubsubMessageSubscriber(appDependencies.subscriber, clusterHelper)
        Stream.eval(logger.info(s"starting subscriber ${subscriberConfig.projectTopicName} in boot")) ++ pubsubSubscriber.process
          .handleErrorWith(
            error => Stream.eval(logger.error(error)("Failed to initialize message processor in Boot. "))
          )
      } else Stream.eval(logger.info(s"Not starting subscriber in boot"))

      val startSubscriber = if (leoExecutionModeConfig.backLeo) {
        Stream.eval(appDependencies.subscriber.start)
      } else Stream.eval(IO.unit)

      val httpServer = for {
        _ <- if (leoExecutionModeConfig.backLeo) {
          clusterHelper.setupDataprocImageGoogleGroup()
        } else IO.unit
        _ <- IO.fromFuture {
          IO {
            Http()
              .bindAndHandle(leoRoutes.route, "0.0.0.0", 8080)
              .onError {
                case t: Throwable =>
                  logger.error(t)("FATAL - failure starting http server").unsafeToFuture()
              }
          }
        }
      } yield ()

      val app = Stream(
        appDependencies.publisherStream, //start the publisher queue .dequeue
        messageProcessorStream, //start subscriber dequeue
        Stream.eval(httpServer), //start http server
        startSubscriber //start asyncly pulling data in subscriber
      ).parJoin(4)

      app
        .handleErrorWith { error =>
          Stream.eval(logger.error(error)("Failed to start leonardo"))
        }
        .compile
        .drain
    }
  }

  private def convertToPubsubMessagePipe[F[_]]: Pipe[F, LeoPubsubMessage, PubsubMessage] =
    in =>
      in.map { msg =>
        val stringMessage = msg.asJson.noSpaces
        val byteString = ByteString.copyFromUtf8(stringMessage)
        PubsubMessage
          .newBuilder()
          .setData(byteString)
          .putAttributes("traceId", msg.traceId.map(_.asString).getOrElse("null"))
          .build()
      }

  private def createDependencies[F[_]: StructuredLogger: ContextShift: ConcurrentEffect: Timer](
    pathToCredentialJson: String
  )(implicit ec: ExecutionContext, as: ActorSystem): Resource[F, AppDependencies[F]] = {
    implicit val metrics = NewRelicMetrics.fromNewRelic[F](applicationConfig.applicationName)
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
      dbRef <- DbReference.init(liquibaseConfig, concurrentDbAccessPermits, blocker)
      clusterDnsCache = new ClusterDnsCache(proxyConfig, dbRef, clusterDnsCacheConfig, blocker)
      welderDao = new HttpWelderDAO[F](clusterDnsCache, clientWithRetryAndLogging)
      dockerDao = HttpDockerDAO[F](clientWithRetryAndLogging)
      jupyterDao = new HttpJupyterDAO[F](clusterDnsCache, clientWithRetryAndLogging)
      rstudioDAO = new HttpRStudioDAO(clusterDnsCache, clientWithRetryAndLogging)
      serviceAccountProvider = new PetClusterServiceAccountProvider(samDao)
      authProvider = new SamAuthProvider(samDao, samAuthConfig, serviceAccountProvider, blocker)

      credentialJson <- org.broadinstitute.dsde.workbench.util2
        .readFile(applicationConfig.leoServiceAccountJsonFile.getAbsolutePath)
        .map(_.toString)
      json = Json(credentialJson)
      jsonWithServiceAccountUser = Json(credentialJson, Option(googleGroupsConfig.googleAdminEmail))

      googleStorageDAO = new HttpGoogleStorageDAO(applicationConfig.applicationName, json, workbenchMetricsBaseName)
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
                                        NetworkTag(proxyConfig.networkTag),
                                        dataprocConfig.dataprocDefaultRegion,
                                        dataprocConfig.dataprocZone)

      googlePublisher <- GooglePublisher.resource[F](publisherConfig)

      publisherQueue <- Resource.liftF(InspectableQueue.bounded[F, LeoPubsubMessage](pubsubConfig.queueSize))

      publisherStream = Stream.eval(Logger[F].info(s"Initializing publisher for ${publisherConfig.projectTopicName}")) ++ (publisherQueue.dequeue through convertToPubsubMessagePipe through googlePublisher.publishNative)

      subscriberQueue <- Resource.liftF(InspectableQueue.bounded[F, Event[LeoPubsubMessage]](pubsubConfig.queueSize))
      subscriber <- GoogleSubscriber.resource(subscriberConfig, subscriberQueue)

      googleComputeService <- GoogleComputeService.resource(pathToCredentialJson, blocker, semaphore)
    } yield AppDependencies(
      storage,
      dbRef,
      clusterDnsCache,
      googleStorageDAO,
      petGoogleStorageDAO,
      googleComputeService,
      googleProjectDAO,
      googleDirectoryDAO,
      googleIamDAO,
      gdDAO,
      samDao,
      welderDao,
      dockerDao,
      jupyterDao,
      rstudioDAO,
      serviceAccountProvider,
      authProvider,
      metrics,
      blocker,
      publisherStream,
      publisherQueue,
      subscriber
    )
  }

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

final case class AppDependencies[F[_]](google2StorageDao: GoogleStorageService[F],
                                       dbReference: DbReference[F],
                                       clusterDnsCache: ClusterDnsCache[F],
                                       googleStorageDAO: HttpGoogleStorageDAO,
                                       petGoogleStorageDAO: String => GoogleStorageDAO,
                                       googleComputeService: GoogleComputeService[F],
                                       googleProjectDAO: HttpGoogleProjectDAO,
                                       googleDirectoryDAO: HttpGoogleDirectoryDAO,
                                       googleIamDAO: HttpGoogleIamDAO,
                                       googleDataprocDAO: HttpGoogleDataprocDAO,
                                       samDAO: HttpSamDAO[F],
                                       welderDAO: HttpWelderDAO[F],
                                       dockerDAO: HttpDockerDAO[F],
                                       jupyterDAO: HttpJupyterDAO[F],
                                       rStudioDAO: RStudioDAO[F],
                                       serviceAccountProvider: ServiceAccountProvider[F],
                                       authProvider: LeoAuthProvider[F],
                                       metrics: NewRelicMetrics[F],
                                       blocker: Blocker,
                                       publisherStream: Stream[F, Unit],
                                       publisherQueue: fs2.concurrent.InspectableQueue[F, LeoPubsubMessage],
                                       subscriber: GoogleSubscriber[F, LeoPubsubMessage])
