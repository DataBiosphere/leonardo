package org.broadinstitute.dsde.workbench.leonardo

import cats.implicits._
import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.stream.ActorMaterializer
import cats.effect.{ExitCode, IO, IOApp, Resource}
import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.LazyLogging
import io.chrisdavenport.log4cats.Logger
import io.chrisdavenport.log4cats.slf4j.Slf4jLogger
import net.ceedubs.ficus.Ficus._
import org.broadinstitute.dsde.workbench.google.GoogleCredentialModes.{Pem, Token}
import org.broadinstitute.dsde.workbench.google.{GoogleStorageDAO, HttpGoogleIamDAO, HttpGoogleProjectDAO, HttpGoogleStorageDAO}
import org.broadinstitute.dsde.workbench.google2.GoogleStorageService
import org.broadinstitute.dsde.workbench.leonardo.api.{LeoRoutes, StandardUserInfoDirectives}
import org.broadinstitute.dsde.workbench.leonardo.auth.{LeoAuthProviderHelper, ServiceAccountProviderHelper}
import org.broadinstitute.dsde.workbench.leonardo.config.{AutoFreezeConfig, ClusterBucketConfig, ClusterDefaultsConfig, ClusterDnsCacheConfig, ClusterFilesConfig, ClusterResourcesConfig, DataprocConfig, LeoExecutionModeConfig, MonitorConfig, ProxyConfig, SamConfig, SwaggerConfig, ZombieClusterConfig}
import org.broadinstitute.dsde.workbench.leonardo.dao.{HttpJupyterDAO, HttpRStudioDAO, HttpSamDAO, HttpWelderDAO}
import org.broadinstitute.dsde.workbench.leonardo.dao.google.{HttpGoogleComputeDAO, HttpGoogleDataprocDAO}
import org.broadinstitute.dsde.workbench.leonardo.db.DbReference
import org.broadinstitute.dsde.workbench.leonardo.dns.ClusterDnsCache
import org.broadinstitute.dsde.workbench.leonardo.model.google.NetworkTag
import org.broadinstitute.dsde.workbench.leonardo.monitor.{ClusterDateAccessedActor, ClusterMonitorSupervisor, ZombieClusterMonitor}
import org.broadinstitute.dsde.workbench.leonardo.service.{LeonardoService, ProxyService, StatusService}
import org.broadinstitute.dsde.workbench.leonardo.util.BucketHelper

object Boot extends IOApp with LazyLogging {
  private def startup(): IO[Unit] = {
    val config = ConfigFactory.parseResources("leonardo.conf").withFallback(ConfigFactory.load())
    val dataprocConfig = config.as[DataprocConfig]("dataproc")
    val proxyConfig = config.as[ProxyConfig]("proxy")
    val swaggerConfig = config.as[SwaggerConfig]("swagger")
    val clusterFilesConfig = config.as[ClusterFilesConfig]("clusterFiles")
    val clusterResourcesConfig = config.as[ClusterResourcesConfig]("clusterResources")
    val clusterDefaultsConfig = config.as[ClusterDefaultsConfig]("clusterDefaults")
    val monitorConfig = config.as[MonitorConfig]("monitor")
    val samConfig = config.as[SamConfig]("sam")
    val autoFreezeConfig = config.as[AutoFreezeConfig]("autoFreeze")
    val contentSecurityPolicy = config.as[Option[String]]("jupyterConfig.contentSecurityPolicy").getOrElse("default-src: 'self'")
    val zombieClusterMonitorConfig = config.as[ZombieClusterConfig]("zombieClusterMonitor")
    val clusterDnsCacheConfig = config.as[ClusterDnsCacheConfig]("clusterDnsCache")
    val leoExecutionModeConfig = config.as[LeoExecutionModeConfig]("leoExecutionMode")
    val clusterBucketConfig = config.as[ClusterBucketConfig]("clusterBucket")

    // we need an ActorSystem to host our application in
    implicit val system = ActorSystem("leonardo")
    implicit val materializer = ActorMaterializer()
    import system.dispatcher

    val serviceAccountProviderClass = config.as[String]("serviceAccounts.providerClass")
    val serviceAccountConfig = config.getConfig("serviceAccounts.providerConfig")
    val serviceAccountProvider = ServiceAccountProviderHelper.create(serviceAccountProviderClass, serviceAccountConfig)

    val leoServiceAccountJsonFile = config.as[String]("google.leoServiceAccountJsonFile")

    val authProviderClass = config.as[String]("auth.providerClass")
    val authConfig = config.getConfig("auth.providerConfig")
    val authProvider = LeoAuthProviderHelper.create(authProviderClass, authConfig, serviceAccountProvider)

    val dbRef = DbReference.init(config)
    system.registerOnTermination {
      dbRef.database.close()
    }

    val petGoogleStorageDAO: String => GoogleStorageDAO = token => {
      new HttpGoogleStorageDAO(dataprocConfig.applicationName, Token(() => token), "google")
    }

    val (leoServiceAccountEmail, leoServiceAccountPemFile) = serviceAccountProvider.getLeoServiceAccountAndKey
    val gdDAO = new HttpGoogleDataprocDAO(dataprocConfig.applicationName, Pem(leoServiceAccountEmail, leoServiceAccountPemFile), "google", NetworkTag(dataprocConfig.networkTag), dataprocConfig.dataprocDefaultRegion, dataprocConfig.dataprocZone, dataprocConfig.defaultExecutionTimeout)
    val googleComputeDAO = new HttpGoogleComputeDAO(dataprocConfig.applicationName, Pem(leoServiceAccountEmail, leoServiceAccountPemFile), "google")
    val googleIamDAO = new HttpGoogleIamDAO(dataprocConfig.applicationName, Pem(leoServiceAccountEmail, leoServiceAccountPemFile), "google")
    val googleStorageDAO = new HttpGoogleStorageDAO(dataprocConfig.applicationName, Pem(leoServiceAccountEmail, leoServiceAccountPemFile), "google")
    val googleProjectDAO = new HttpGoogleProjectDAO(dataprocConfig.applicationName, Pem(leoServiceAccountEmail, leoServiceAccountPemFile), "google")
    val clusterDnsCache = new ClusterDnsCache(proxyConfig, dbRef, clusterDnsCacheConfig)
    val bucketHelper = new BucketHelper(dataprocConfig, gdDAO, googleComputeDAO, googleStorageDAO, serviceAccountProvider)
    implicit def unsafeLogger = Slf4jLogger.getLogger[IO]

    createDependencies(leoServiceAccountJsonFile).use {
      appDependencies =>
        val welderDao = new HttpWelderDAO(clusterDnsCache)
        val leonardoService = new LeonardoService(dataprocConfig, welderDao, clusterFilesConfig, clusterResourcesConfig, clusterDefaultsConfig, proxyConfig, swaggerConfig, autoFreezeConfig, gdDAO, googleComputeDAO, googleIamDAO, googleProjectDAO, googleStorageDAO, petGoogleStorageDAO, dbRef, authProvider, serviceAccountProvider, bucketHelper, contentSecurityPolicy)
        if(leoExecutionModeConfig.backLeo) {
          val jupyterDAO = new HttpJupyterDAO(clusterDnsCache)
          val rstudioDAO = new HttpRStudioDAO(clusterDnsCache)
          val clusterMonitorSupervisor = system.actorOf(ClusterMonitorSupervisor.props(monitorConfig, dataprocConfig, clusterBucketConfig, gdDAO, googleComputeDAO, googleIamDAO, googleStorageDAO, appDependencies.google2StorageDao, dbRef, authProvider, autoFreezeConfig, jupyterDAO, rstudioDAO, leonardoService))
          val zombieClusterMonitor = system.actorOf(ZombieClusterMonitor.props(zombieClusterMonitorConfig, gdDAO, googleProjectDAO, dbRef))
        }
        val samDAO = new HttpSamDAO(samConfig.server)
        val clusterDateAccessedActor = system.actorOf(ClusterDateAccessedActor.props(autoFreezeConfig, dbRef))
        val proxyService = new ProxyService(proxyConfig, gdDAO, dbRef, clusterDnsCache, authProvider, clusterDateAccessedActor)
        val statusService = new StatusService(gdDAO, samDAO, dbRef, dataprocConfig)
        val leoRoutes = new LeoRoutes(leonardoService, proxyService, statusService, swaggerConfig) with StandardUserInfoDirectives
        IO.fromFuture(IO(Http().bindAndHandle(leoRoutes.route, "0.0.0.0", 8080)
          .recover {
            case t: Throwable =>
              logger.error("FATAL - failure starting http server", t)
              throw t
          }.void))
    }
  }

  def createDependencies(pathToCredentialJson: String)(implicit logger: Logger[IO]): Resource[IO, AppDependencies] = GoogleStorageService.resource[IO](pathToCredentialJson, scala.concurrent.ExecutionContext.global).map(storage => AppDependencies(storage))

  override def run(args: List[String]): IO[ExitCode] = startup().as(ExitCode.Success)
}

final case class AppDependencies(google2StorageDao: GoogleStorageService[IO])
