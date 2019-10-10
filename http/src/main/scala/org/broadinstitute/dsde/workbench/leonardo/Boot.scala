package org.broadinstitute.dsde.workbench.leonardo

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.stream.ActorMaterializer
import cats.effect.concurrent.Semaphore
import cats.effect.{Blocker, ConcurrentEffect, ContextShift, ExitCode, IO, IOApp, Resource, Timer}
import cats.implicits._
import com.typesafe.scalalogging.LazyLogging
import io.chrisdavenport.log4cats.Logger
import io.chrisdavenport.log4cats.slf4j.Slf4jLogger
import org.broadinstitute.dsde.workbench.google.GoogleCredentialModes.{Pem, Token}
import org.broadinstitute.dsde.workbench.google.{GoogleStorageDAO, HttpGoogleIamDAO, HttpGoogleProjectDAO, HttpGoogleStorageDAO}
import org.broadinstitute.dsde.workbench.google2.GoogleStorageService
import org.broadinstitute.dsde.workbench.leonardo.api.{LeoRoutes, StandardUserInfoDirectives}
import org.broadinstitute.dsde.workbench.leonardo.auth.sam.{PetClusterServiceAccountProvider, SamAuthProvider}
import org.broadinstitute.dsde.workbench.leonardo.config.Config._
import org.broadinstitute.dsde.workbench.leonardo.dao.google.{HttpGoogleComputeDAO, HttpGoogleDataprocDAO}
import org.broadinstitute.dsde.workbench.leonardo.dao.{HttpJupyterDAO, HttpRStudioDAO, HttpSamDAO, HttpWelderDAO}
import org.broadinstitute.dsde.workbench.leonardo.db.DbReference
import org.broadinstitute.dsde.workbench.leonardo.dns.ClusterDnsCache
import org.broadinstitute.dsde.workbench.leonardo.model.google.NetworkTag
import org.broadinstitute.dsde.workbench.leonardo.model.{ClusterTool, LeoAuthProvider}
import org.broadinstitute.dsde.workbench.leonardo.monitor.{ClusterDateAccessedActor, ClusterMonitorSupervisor, ClusterToolMonitor, ZombieClusterMonitor}
import org.broadinstitute.dsde.workbench.leonardo.service.{LeonardoService, ProxyService, StatusService}
import org.broadinstitute.dsde.workbench.leonardo.util.{BucketHelper, ClusterHelper}
import org.broadinstitute.dsde.workbench.util.ExecutionContexts
import org.http4s.client.blaze
import org.http4s.client.middleware.{Retry, RetryPolicy, Logger => Http4sLogger}

import scala.concurrent.duration._

object Boot extends IOApp with LazyLogging {
  private def startup(): IO[Unit] = {
    // we need an ActorSystem to host our application in
    implicit val system = ActorSystem("leonardo")
    implicit val materializer = ActorMaterializer()
    import system.dispatcher

    val dbRef = DbReference.init(liquibaseConfig)
    system.registerOnTermination {
      dbRef.database.close()
    }

    val petGoogleStorageDAO: String => GoogleStorageDAO = token => {
      new HttpGoogleStorageDAO(dataprocConfig.applicationName, Token(() => token), "google")
    }

    val pem = Pem(serviceAccountProviderConfig.leoServiceAccount, serviceAccountProviderConfig.leoPemFile)
    val gdDAO = new HttpGoogleDataprocDAO(dataprocConfig.applicationName, pem, "google", NetworkTag(dataprocConfig.networkTag), dataprocConfig.dataprocDefaultRegion, dataprocConfig.dataprocZone)
    val googleComputeDAO = new HttpGoogleComputeDAO(dataprocConfig.applicationName, pem, "google")
    val googleIamDAO = new HttpGoogleIamDAO(dataprocConfig.applicationName, pem, "google")
    val googleStorageDAO = new HttpGoogleStorageDAO(dataprocConfig.applicationName, pem, "google")
    val googleProjectDAO = new HttpGoogleProjectDAO(dataprocConfig.applicationName, pem, "google")
    val clusterDnsCache = new ClusterDnsCache(proxyConfig, dbRef, clusterDnsCacheConfig)
    val clusterHelper = new ClusterHelper(dbRef, dataprocConfig, gdDAO, googleComputeDAO, googleIamDAO)
    implicit def unsafeLogger = Slf4jLogger.getLogger[IO]

    createDependencies[IO](leoServiceAccountJsonFile).use {
      appDependencies =>
        val serviceAccountProvider = new PetClusterServiceAccountProvider[IO](appDependencies.samDAO)
        val bucketHelper = new BucketHelper(dataprocConfig, gdDAO, googleComputeDAO, googleStorageDAO, serviceAccountProvider)
        val authProvider: LeoAuthProvider[IO] = new SamAuthProvider(appDependencies.samDAO, samAuthConfig, serviceAccountProvider)
        val welderDao = new HttpWelderDAO(clusterDnsCache)
        val leonardoService = new LeonardoService(dataprocConfig, welderDao, clusterFilesConfig, clusterResourcesConfig, clusterDefaultsConfig, proxyConfig, swaggerConfig, autoFreezeConfig, gdDAO, googleComputeDAO, googleProjectDAO, googleStorageDAO, petGoogleStorageDAO, dbRef, authProvider, serviceAccountProvider, bucketHelper, clusterHelper, contentSecurityPolicy)
        if(leoExecutionModeConfig.backLeo) {
          val jupyterDAO = new HttpJupyterDAO(clusterDnsCache)
          val rstudioDAO = new HttpRStudioDAO(clusterDnsCache)
          system.actorOf(ClusterMonitorSupervisor.props(monitorConfig, dataprocConfig, clusterBucketConfig, gdDAO, googleComputeDAO, googleStorageDAO, appDependencies.google2StorageDao, dbRef, authProvider, autoFreezeConfig, jupyterDAO, rstudioDAO, welderDao, leonardoService, clusterHelper))
          system.actorOf(ZombieClusterMonitor.props(zombieClusterMonitorConfig, gdDAO, googleProjectDAO, dbRef))
          system.actorOf(ClusterToolMonitor.props(clusterToolMonitorConfig, gdDAO, googleProjectDAO, dbRef, Map(ClusterTool.Jupyter ->  jupyterDAO, ClusterTool.Welder -> welderDao), Metrics.newRelic))
        }
        val clusterDateAccessedActor = system.actorOf(ClusterDateAccessedActor.props(autoFreezeConfig, dbRef))
        val proxyService = new ProxyService(proxyConfig, gdDAO, dbRef, clusterDnsCache, authProvider, clusterDateAccessedActor)
        val statusService = new StatusService(gdDAO, appDependencies.samDAO, dbRef, dataprocConfig)
        val leoRoutes = new LeoRoutes(leonardoService, proxyService, statusService, swaggerConfig) with StandardUserInfoDirectives
        IO.fromFuture(IO(Http().bindAndHandle(leoRoutes.route, "0.0.0.0", 8080)
          .recover {
            case t: Throwable =>
              logger.error("FATAL - failure starting http server", t)
              throw t
          }.void)) >> IO.never
    }
  }

  def createDependencies[F[_]: Logger: ContextShift: ConcurrentEffect: Timer](pathToCredentialJson: String): Resource[F, AppDependencies[F]] = for {
    blockingEc <- ExecutionContexts.cachedThreadPool[F]
    blocker = Blocker.liftExecutionContext(blockingEc)
    semaphore <- Resource.liftF(Semaphore[F](255L))
    storage <- GoogleStorageService.resource[F](pathToCredentialJson, blocker, Some(semaphore))
    httpClient <- blaze.BlazeClientBuilder[F](
      blockingEc
    ).resource
    retryPolicy = RetryPolicy[F](RetryPolicy.exponentialBackoff(10 seconds, 5))
    clientWithRetry = Retry(retryPolicy)(httpClient)
    clientWithRetryAndLogging = Http4sLogger(logHeaders = true, logBody = false)(clientWithRetry)
    samDao = new HttpSamDAO[F](clientWithRetryAndLogging, httpSamDap2Config)
  } yield AppDependencies(storage, samDao)

  override def run(args: List[String]): IO[ExitCode] = startup().as(ExitCode.Success)
}

final case class AppDependencies[F[_]](google2StorageDao: GoogleStorageService[F], samDAO: HttpSamDAO[F])
