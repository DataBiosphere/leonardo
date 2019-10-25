package org.broadinstitute.dsde.workbench.leonardo

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.stream.ActorMaterializer
import cats.effect.concurrent.Semaphore
import cats.effect.{Blocker, ConcurrentEffect, ContextShift, ExitCode, IO, IOApp, Resource, Timer}
import cats.implicits._
import com.typesafe.scalalogging.LazyLogging
import com.typesafe.sslconfig.akka.AkkaSSLConfig
import com.typesafe.sslconfig.akka.util.AkkaLoggerFactory
import com.typesafe.sslconfig.ssl.ConfigSSLContextBuilder
import io.chrisdavenport.log4cats.Logger
import io.chrisdavenport.log4cats.slf4j.Slf4jLogger
import org.broadinstitute.dsde.workbench.google.GoogleCredentialModes.{Pem, Token}
import org.broadinstitute.dsde.workbench.google.{
  GoogleStorageDAO,
  HttpGoogleIamDAO,
  HttpGoogleProjectDAO,
  HttpGoogleStorageDAO
}
import org.broadinstitute.dsde.workbench.google2.GoogleStorageService
import org.broadinstitute.dsde.workbench.leonardo.api.{LeoRoutes, StandardUserInfoDirectives}
import org.broadinstitute.dsde.workbench.leonardo.auth.sam.{PetClusterServiceAccountProvider, SamAuthProvider}
import org.broadinstitute.dsde.workbench.leonardo.config.Config._
import org.broadinstitute.dsde.workbench.leonardo.dao._
import org.broadinstitute.dsde.workbench.leonardo.dao.google.{HttpGoogleComputeDAO, HttpGoogleDataprocDAO}
import org.broadinstitute.dsde.workbench.leonardo.db.DbReference
import org.broadinstitute.dsde.workbench.leonardo.dns.ClusterDnsCache
import org.broadinstitute.dsde.workbench.leonardo.model.LeoAuthProvider
import org.broadinstitute.dsde.workbench.leonardo.model.google.NetworkTag
import org.broadinstitute.dsde.workbench.leonardo.monitor.{
  ClusterDateAccessedActor,
  ClusterMonitorSupervisor,
  ClusterToolMonitor,
  ZombieClusterMonitor
}
import org.broadinstitute.dsde.workbench.leonardo.service.{LeonardoService, ProxyService, StatusService}
import org.broadinstitute.dsde.workbench.leonardo.util.{BucketHelper, ClusterHelper}
import org.broadinstitute.dsde.workbench.newrelic.NewRelicMetrics
import org.broadinstitute.dsde.workbench.util.ExecutionContexts
import org.http4s.client.blaze
import org.http4s.client.middleware.{Retry, RetryPolicy, Logger => Http4sLogger}

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._

object Boot extends IOApp with LazyLogging {
  private def startup(): IO[Unit] = {
    // we need an ActorSystem to host our application in
    implicit val system = ActorSystem("leonardo")
    implicit val materializer = ActorMaterializer()
    import system.dispatcher

    val petGoogleStorageDAO: String => GoogleStorageDAO = token => {
      new HttpGoogleStorageDAO(dataprocConfig.applicationName, Token(() => token), "google")
    }

    val pem = Pem(serviceAccountProviderConfig.leoServiceAccount, serviceAccountProviderConfig.leoPemFile)
    val googleStorageDAO = new HttpGoogleStorageDAO(dataprocConfig.applicationName, pem, "google")
    val googleProjectDAO = new HttpGoogleProjectDAO(dataprocConfig.applicationName, pem, "google")
    implicit def unsafeLogger = Slf4jLogger.getLogger[IO]

    createDependencies(leoServiceAccountJsonFile, pem).use { appDependencies =>
      implicit val metrics = appDependencies.metrics
      val serviceAccountProvider = new PetClusterServiceAccountProvider[IO](appDependencies.samDAO)
      val authProvider: LeoAuthProvider[IO] =
        new SamAuthProvider(appDependencies.samDAO, samAuthConfig, serviceAccountProvider)
      val bucketHelper = new BucketHelper(dataprocConfig,
                                          appDependencies.googleDataprocDAO,
                                          appDependencies.googleComputeDAO,
                                          googleStorageDAO,
                                          serviceAccountProvider)
      val leonardoService = new LeonardoService(dataprocConfig,
                                                appDependencies.welderDAO,
                                                clusterFilesConfig,
                                                clusterResourcesConfig,
                                                clusterDefaultsConfig,
                                                proxyConfig,
                                                swaggerConfig,
                                                autoFreezeConfig,
                                                appDependencies.googleDataprocDAO,
                                                appDependencies.googleComputeDAO,
                                                googleProjectDAO,
                                                googleStorageDAO,
                                                petGoogleStorageDAO,
                                                appDependencies.dbReference,
                                                authProvider,
                                                serviceAccountProvider,
                                                bucketHelper,
                                                appDependencies.clusterHelper,
                                                contentSecurityPolicy)
      if (leoExecutionModeConfig.backLeo) {
        val jupyterDAO = new HttpJupyterDAO(appDependencies.clusterDnsCache)
        val rstudioDAO = new HttpRStudioDAO(appDependencies.clusterDnsCache)
        implicit def clusterToolToToolDao =
          ToolDAO.clusterToolToToolDao(jupyterDAO, appDependencies.welderDAO, rstudioDAO)
        system.actorOf(
          ClusterMonitorSupervisor.props(
            monitorConfig,
            dataprocConfig,
            clusterBucketConfig,
            appDependencies.googleDataprocDAO,
            appDependencies.googleComputeDAO,
            googleStorageDAO,
            appDependencies.google2StorageDao,
            appDependencies.dbReference,
            authProvider,
            autoFreezeConfig,
            jupyterDAO,
            rstudioDAO,
            appDependencies.welderDAO,
            leonardoService,
            appDependencies.clusterHelper
          )
        )
        system.actorOf(
          ZombieClusterMonitor.props(zombieClusterMonitorConfig,
                                     appDependencies.googleDataprocDAO,
                                     googleProjectDAO,
                                     appDependencies.dbReference)
        )
        system.actorOf(
          ClusterToolMonitor.props(clusterToolMonitorConfig,
                                   appDependencies.googleDataprocDAO,
                                   googleProjectDAO,
                                   appDependencies.dbReference,
                                   appDependencies.metrics)
        )
      }
      val clusterDateAccessedActor =
        system.actorOf(ClusterDateAccessedActor.props(autoFreezeConfig, appDependencies.dbReference))
      val proxyService = new ProxyService(proxyConfig,
                                          appDependencies.googleDataprocDAO,
                                          appDependencies.dbReference,
                                          appDependencies.clusterDnsCache,
                                          authProvider,
                                          clusterDateAccessedActor)
      val statusService = new StatusService(appDependencies.googleDataprocDAO,
                                            appDependencies.samDAO,
                                            appDependencies.dbReference,
                                            dataprocConfig)
      val leoRoutes = new LeoRoutes(leonardoService, proxyService, statusService, swaggerConfig)
      with StandardUserInfoDirectives
      IO.fromFuture(
        IO(
          Http()
            .bindAndHandle(leoRoutes.route, "0.0.0.0", 8080)
            .recover {
              case t: Throwable =>
                logger.error("FATAL - failure starting http server", t)
                throw t
            }
            .void
        )
      ) >> IO.never
    }
  }

  def createDependencies[F[_]: Logger: ContextShift: ConcurrentEffect: Timer](
    pathToCredentialJson: String,
    pem: Pem
  )(implicit ec: ExecutionContext, as: ActorSystem): Resource[F, AppDependencies[F]] = {
    implicit val metrics = NewRelicMetrics.fromNewRelic[F]("leonardo")

    for {
      blockingEc <- ExecutionContexts.cachedThreadPool[F]
      semaphore <- Resource.liftF(Semaphore[F](255L))
      blocker = Blocker.liftExecutionContext(blockingEc)
      storage <- GoogleStorageService.resource[F](pathToCredentialJson, blocker, Some(semaphore))
      retryPolicy = RetryPolicy[F](RetryPolicy.exponentialBackoff(30 seconds, 5))

      sslContext = getSSLContext
      httpClientWithCustomSSL <- blaze.BlazeClientBuilder[F](blockingEc, Some(sslContext)).resource
      clientWithRetryWithCustomSSL = Retry(retryPolicy)(httpClientWithCustomSSL)
      clientWithRetryAndLogging = Http4sLogger[F](logHeaders = true, logBody = false)(clientWithRetryWithCustomSSL)

      samDao = new HttpSamDAO[F](clientWithRetryAndLogging, httpSamDap2Config)
      dbRef <- Resource.make(ConcurrentEffect[F].delay(DbReference.init(liquibaseConfig)))(
        db => ConcurrentEffect[F].delay(db.database.close)
      )
      clusterDnsCache = new ClusterDnsCache(proxyConfig, dbRef, clusterDnsCacheConfig)
      welderDao = new HttpWelderDAO[F](clusterDnsCache, clientWithRetryAndLogging)

      googleIamDAO = new HttpGoogleIamDAO(dataprocConfig.applicationName, pem, "google")
      googleComputeDAO = new HttpGoogleComputeDAO(dataprocConfig.applicationName, pem, "google")
      gdDAO = new HttpGoogleDataprocDAO(dataprocConfig.applicationName,
                                        pem,
                                        "google",
                                        NetworkTag(dataprocConfig.networkTag),
                                        dataprocConfig.dataprocDefaultRegion,
                                        dataprocConfig.dataprocZone)
      clusterHelper = new ClusterHelper(dbRef, dataprocConfig, gdDAO, googleComputeDAO, googleIamDAO)
    } yield AppDependencies(storage,
                            dbRef,
                            clusterDnsCache,
                            googleComputeDAO,
                            gdDAO,
                            samDao,
                            welderDao,
                            clusterHelper,
                            metrics)
  }

  private def getSSLContext(implicit actorSystem: ActorSystem) = {
    val akkaSSLConfig = AkkaSSLConfig()
    val config = akkaSSLConfig.config
    val logger = new AkkaLoggerFactory(actorSystem)
    new ConfigSSLContextBuilder(logger,
                                config,
                                akkaSSLConfig.buildKeyManagerFactory(config),
                                akkaSSLConfig.buildTrustManagerFactory(config)).build()
  }

  override def run(args: List[String]): IO[ExitCode] = startup().as(ExitCode.Success)
}

final case class AppDependencies[F[_]](google2StorageDao: GoogleStorageService[F],
                                       dbReference: DbReference,
                                       clusterDnsCache: ClusterDnsCache,
                                       googleComputeDAO: HttpGoogleComputeDAO,
                                       googleDataprocDAO: HttpGoogleDataprocDAO,
                                       samDAO: HttpSamDAO[F],
                                       welderDAO: HttpWelderDAO[F],
                                       clusterHelper: ClusterHelper,
                                       metrics: NewRelicMetrics[F])
