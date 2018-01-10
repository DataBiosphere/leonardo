package org.broadinstitute.dsde.workbench.leonardo

import akka.actor.{ActorRef, ActorSystem}
import akka.http.scaladsl.Http
import akka.stream.ActorMaterializer
import com.typesafe.config.{Config, ConfigFactory}
import com.typesafe.scalalogging.LazyLogging
import net.ceedubs.ficus.Ficus._
import org.broadinstitute.dsde.workbench.google.HttpGoogleIamDAO
import org.broadinstitute.dsde.workbench.leonardo.api.{LeoRoutes, StandardUserInfoDirectives}
import org.broadinstitute.dsde.workbench.leonardo.dao.{GoogleDataprocDAO, HttpSamDAO}
import org.broadinstitute.dsde.workbench.leonardo.config.{ClusterDefaultsConfig, ClusterFilesConfig, ClusterResourcesConfig, DataprocConfig, MonitorConfig, ProxyConfig, SamConfig, SwaggerConfig}
import org.broadinstitute.dsde.workbench.leonardo.db.DbReference
import org.broadinstitute.dsde.workbench.leonardo.dns.ClusterDnsCache
import org.broadinstitute.dsde.workbench.leonardo.model.{ClusterStatus, LeoAuthProvider, ServiceAccountProvider}
import org.broadinstitute.dsde.workbench.leonardo.monitor.ClusterMonitorSupervisor
import org.broadinstitute.dsde.workbench.leonardo.monitor.ClusterMonitorSupervisor._
import org.broadinstitute.dsde.workbench.leonardo.service.{LeonardoService, ProxyService, StatusService}

import scala.concurrent.ExecutionContext
import scala.util.{Failure, Success}

object Boot extends App with LazyLogging {
  private def startup(): Unit = {

    // Specifies the name service providers to use, in priority order.
    // "dns,Jupyter" maps to the org.broadinstitute.dsde.workbench.leonardo.dns.JupyterNameService class.
    // "default" is the Java default name service.
    //
    // We do this so we can map *.jupyter-{{env}}.firecloud.org names to real Dataproc IP addresses, and
    // still pass SSL wildcard certificate verification. If we change this to not use a wildcard cert
    // and instead generate a new cert per Dataproc cluster, then we could probably generate the certs
    // based on the IP address and get rid of this code.
    //
    // See also: http://docs.oracle.com/javase/8/docs/technotes/guides/net/properties.html
    System.setProperty("sun.net.spi.nameservice.provider.1", "dns,Jupyter")
    System.setProperty("sun.net.spi.nameservice.provider.2", "default")

    val config = ConfigFactory.parseResources("leonardo.conf").withFallback(ConfigFactory.load())
    val dataprocConfig = config.as[DataprocConfig]("dataproc")
    val proxyConfig = config.as[ProxyConfig]("proxy")
    val swaggerConfig = config.as[SwaggerConfig]("swagger")
    val clusterFilesConfig = config.as[ClusterFilesConfig]("clusterFiles")
    val clusterResourcesConfig = config.as[ClusterResourcesConfig]("clusterResources")
    val clusterDefaultsConfig = config.as[ClusterDefaultsConfig]("clusterDefaults")
    val monitorConfig = config.as[MonitorConfig]("monitor")
    val samConfig = config.as[SamConfig]("sam")

    val serviceAccountProviderClass = config.as[String]("serviceAccounts.providerClass")
    val serviceAccountConfig = config.getConfig("serviceAccounts.config")
    val serviceAccountProvider = constructProvider[ServiceAccountProvider](serviceAccountProviderClass, serviceAccountConfig)

    val authProviderClass = config.as[String]("auth.providerClass")
    val authConfig = config.getConfig("auth.providerConfig")
    val authProvider = constructProvider[LeoAuthProvider](authProviderClass, authConfig, serviceAccountProvider)

    // we need an ActorSystem to host our application in
    implicit val system = ActorSystem("leonardo")
    implicit val materializer = ActorMaterializer()
    import system.dispatcher

    val dbRef = DbReference.init(config)
    system.registerOnTermination {
      dbRef.database.close()
    }

    val (leoServiceAccountEmail, leoServiceAccountPemFile) = serviceAccountProvider.getLeoServiceAccountAndKey
    val gdDAO = new GoogleDataprocDAO(leoServiceAccountEmail, leoServiceAccountPemFile, dataprocConfig, proxyConfig, clusterDefaultsConfig, clusterFilesConfig, clusterResourcesConfig)
    val googleIamDAO = new HttpGoogleIamDAO(leoServiceAccountEmail.value, leoServiceAccountPemFile.getAbsolutePath, dataprocConfig.applicationName, "google")
    val samDAO = new HttpSamDAO(samConfig.server)
    val clusterDnsCache = system.actorOf(ClusterDnsCache.props(proxyConfig, dbRef))
    val clusterMonitorSupervisor = system.actorOf(ClusterMonitorSupervisor.props(monitorConfig, dataprocConfig, gdDAO, googleIamDAO, dbRef, clusterDnsCache))
    val leonardoService = new LeonardoService(dataprocConfig, clusterFilesConfig, clusterResourcesConfig, proxyConfig, swaggerConfig, gdDAO, googleIamDAO, dbRef, clusterMonitorSupervisor, authProvider, serviceAccountProvider)
    val proxyService = new ProxyService(proxyConfig, gdDAO, dbRef, clusterDnsCache, authProvider)
    val statusService = new StatusService(gdDAO, samDAO, dbRef, dataprocConfig)
    val leoRoutes = new LeoRoutes(leonardoService, proxyService, statusService, swaggerConfig) with StandardUserInfoDirectives

    startClusterMonitors(dbRef, clusterMonitorSupervisor)

    Http().bindAndHandle(leoRoutes.route, "0.0.0.0", 8080)
      .recover {
        case t: Throwable =>
          logger.error("FATAL - failure starting http server", t)
          throw t
      }
  }

  private def constructProvider[T](className: String, config: Config): T = {
    Class.forName(className)
      .getConstructor(classOf[Config])
      .newInstance(config)
      .asInstanceOf[T]
  }


  private def constructProvider[T](className: String,  config: Config, serviceAccountProvider: ServiceAccountProvider): T = {
    Class.forName(className)
      .getConstructor(classOf[Config], classOf[ServiceAccountProvider])
      .newInstance(config, serviceAccountProvider)
      .asInstanceOf[T]
  }

  private def startClusterMonitors(dbRef: DbReference, clusterMonitor: ActorRef)(implicit executionContext: ExecutionContext) = {
    dbRef.inTransaction { dataAccess =>
      dataAccess.clusterQuery.listMonitored
    } onComplete {
      case Success(clusters) =>
        clusters.foreach {
          case c if c.status == ClusterStatus.Deleting => clusterMonitor ! ClusterDeleted(c)
          case c => clusterMonitor ! ClusterCreated(c)
        }
      case Failure(e) =>
        logger.error("Error starting cluster monitor", e)
    }
  }

  startup()
}
