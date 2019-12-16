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
  HttpGoogleDirectoryDAO,
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
import org.broadinstitute.dsde.workbench.leonardo.model.google.NetworkTag
import org.broadinstitute.dsde.workbench.leonardo.model.{LeoAuthProvider, ServiceAccountProvider}
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
  val workbenchMetricsBaseName = "google"

  private def startup(): IO[Unit] = {
    // We need an ActorSystem to host our application in
    implicit val system = ActorSystem("leonardo")
    implicit val materializer = ActorMaterializer()
    import system.dispatcher

    val petGoogleStorageDAO: String => GoogleStorageDAO = token => {
      new HttpGoogleStorageDAO(dataprocConfig.applicationName, Token(() => token), workbenchMetricsBaseName)
    }

    val pem = Pem(serviceAccountProviderConfig.leoServiceAccount, serviceAccountProviderConfig.leoPemFile)
    // We need the Pem below for DirectoryDAO to be able to make user-impersonating calls (e.g. createGroup)
    val pemWithServiceAccountUser =
      Pem(pem.serviceAccountClientId, pem.pemFile, Option(googleGroupsConfig.googleAdminEmail))
    implicit def unsafeLogger = Slf4jLogger.getLogger[IO]

    createDependencies(leoServiceAccountJsonFile, pem, pemWithServiceAccountUser).use { appDependencies =>
      implicit val metrics = appDependencies.metrics

      val bucketHelper = new BucketHelper(appDependencies.googleComputeDAO,
                                          appDependencies.googleStorageDAO,
                                          appDependencies.google2StorageDao,
                                          appDependencies.serviceAccountProvider)

      val clusterHelper = new ClusterHelper(appDependencies.dbReference,
                                            dataprocConfig,
                                            googleGroupsConfig,
                                            proxyConfig,
                                            clusterResourcesConfig,
                                            clusterFilesConfig,
                                            monitorConfig,
                                            bucketHelper,
                                            appDependencies.googleDataprocDAO,
                                            appDependencies.googleComputeDAO,
                                            appDependencies.googleDirectoryDAO,
                                            appDependencies.googleIamDAO,
                                            appDependencies.googleProjectDAO,
                                            appDependencies.blocker)

      val leonardoService = new LeonardoService(dataprocConfig,
                                                appDependencies.welderDAO,
                                                clusterDefaultsConfig,
                                                proxyConfig,
                                                swaggerConfig,
                                                autoFreezeConfig,
                                                petGoogleStorageDAO,
                                                appDependencies.dbReference,
                                                appDependencies.authProvider,
                                                appDependencies.serviceAccountProvider,
                                                bucketHelper,
                                                clusterHelper,
                                                appDependencies.dockerDAO)
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
            appDependencies.googleStorageDAO,
            appDependencies.google2StorageDao,
            appDependencies.dbReference,
            appDependencies.authProvider,
            autoFreezeConfig,
            jupyterDAO,
            rstudioDAO,
            appDependencies.welderDAO,
            clusterHelper
          )
        )
        system.actorOf(
          ZombieClusterMonitor.props(zombieClusterMonitorConfig,
                                     appDependencies.googleDataprocDAO,
                                     appDependencies.googleProjectDAO,
                                     appDependencies.dbReference)
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
                                          appDependencies.dbReference,
                                          appDependencies.clusterDnsCache,
                                          appDependencies.authProvider,
                                          clusterDateAccessedActor)
      val statusService = new StatusService(appDependencies.googleDataprocDAO,
                                            appDependencies.samDAO,
                                            appDependencies.dbReference,
                                            dataprocConfig)
      val leoRoutes = new LeoRoutes(leonardoService, proxyService, statusService, swaggerConfig, contentSecurityPolicy)
      with StandardUserInfoDirectives

      (for {
        _ <- if (leoExecutionModeConfig.backLeo) clusterHelper.setupDataprocImageGoogleGroup() else IO.unit
        _ <- IO.fromFuture {
          IO {
            Http()
              .bindAndHandle(leoRoutes.route, "0.0.0.0", 8080)
              .onError {
                case t: Throwable =>
                  unsafeLogger.error(t)("FATAL - failure starting http server").unsafeToFuture()
              }
          }
        }
      } yield ()) >> IO.never
    }
  }

  private def createDependencies[F[_]: Logger: ContextShift: ConcurrentEffect: Timer](
    pathToCredentialJson: String,
    pem: Pem,
    pemWithServiceAccountUser: Pem
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

      samDao = HttpSamDAO[F](clientWithRetryAndLogging, httpSamDap2Config, blocker)
      dbRef <- Resource.make(ConcurrentEffect[F].delay(DbReference.init(liquibaseConfig)))(
        db => ConcurrentEffect[F].delay(db.database.close)
      )
      clusterDnsCache = new ClusterDnsCache(proxyConfig, dbRef, clusterDnsCacheConfig)
      welderDao = new HttpWelderDAO[F](clusterDnsCache, clientWithRetryAndLogging)
      dockerDao = HttpDockerDAO[F](clientWithRetryAndLogging)
      serviceAccountProvider = new PetClusterServiceAccountProvider(samDao)
      authProvider = new SamAuthProvider(samDao, samAuthConfig, serviceAccountProvider, blocker)

      googleStorageDAO = new HttpGoogleStorageDAO(dataprocConfig.applicationName, pem, workbenchMetricsBaseName)
      googleIamDAO = new HttpGoogleIamDAO(dataprocConfig.applicationName, pem, workbenchMetricsBaseName)
      googleComputeDAO = new HttpGoogleComputeDAO(dataprocConfig.applicationName, pem, workbenchMetricsBaseName)
      googleDirectoryDAO = new HttpGoogleDirectoryDAO(dataprocConfig.applicationName,
                                                      pemWithServiceAccountUser,
                                                      workbenchMetricsBaseName)
      googleProjectDAO = new HttpGoogleProjectDAO(dataprocConfig.applicationName, pem, workbenchMetricsBaseName)
      gdDAO = new HttpGoogleDataprocDAO(dataprocConfig.applicationName,
                                        pem,
                                        workbenchMetricsBaseName,
                                        NetworkTag(dataprocConfig.networkTag),
                                        dataprocConfig.dataprocDefaultRegion,
                                        dataprocConfig.dataprocZone)
    } yield AppDependencies(
      storage,
      dbRef,
      clusterDnsCache,
      googleStorageDAO,
      googleComputeDAO,
      googleProjectDAO,
      googleDirectoryDAO,
      googleIamDAO,
      gdDAO,
      samDao,
      welderDao,
      dockerDao,
      serviceAccountProvider,
      authProvider,
      metrics,
      blocker
    )
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
                                       googleStorageDAO: HttpGoogleStorageDAO,
                                       googleComputeDAO: HttpGoogleComputeDAO,
                                       googleProjectDAO: HttpGoogleProjectDAO,
                                       googleDirectoryDAO: HttpGoogleDirectoryDAO,
                                       googleIamDAO: HttpGoogleIamDAO,
                                       googleDataprocDAO: HttpGoogleDataprocDAO,
                                       samDAO: HttpSamDAO[F],
                                       welderDAO: HttpWelderDAO[F],
                                       dockerDAO: HttpDockerDAO[F],
                                       serviceAccountProvider: ServiceAccountProvider[F],
                                       authProvider: LeoAuthProvider[F],
                                       metrics: NewRelicMetrics[F],
                                       blocker: Blocker)
