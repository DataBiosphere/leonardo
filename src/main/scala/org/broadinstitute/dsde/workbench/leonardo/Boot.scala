package org.broadinstitute.dsde.workbench.leonardo

import akka.actor.{ActorRef, ActorSystem}
import akka.http.scaladsl.Http
import akka.stream.ActorMaterializer
import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.LazyLogging
import net.ceedubs.ficus.Ficus._
import org.broadinstitute.dsde.workbench.leonardo.api.LeoRoutes
import org.broadinstitute.dsde.workbench.leonardo.config.{ClusterResourcesConfig, DataprocConfig, MonitorConfig, ProxyConfig, SwaggerConfig}
import org.broadinstitute.dsde.workbench.leonardo.dao.GoogleDataprocDAO
import org.broadinstitute.dsde.workbench.leonardo.db.DbReference
import org.broadinstitute.dsde.workbench.leonardo.dns.ClusterDnsCache
import org.broadinstitute.dsde.workbench.leonardo.model.ClusterStatus
import org.broadinstitute.dsde.workbench.leonardo.monitor.ClusterMonitorSupervisor
import org.broadinstitute.dsde.workbench.leonardo.monitor.ClusterMonitorSupervisor._
import org.broadinstitute.dsde.workbench.leonardo.service.{LeonardoService, ProxyService}

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
    val clusterResourcesConfig = config.as[ClusterResourcesConfig]("clusterResources")
    val monitorConfig = config.as[MonitorConfig]("monitor")

    // we need an ActorSystem to host our application in
    implicit val system = ActorSystem("leonardo")
    implicit val materializer = ActorMaterializer()
    import system.dispatcher

    val dbRef = DbReference.init(config)
    system.registerOnTermination {
      dbRef.database.close()
    }

    val gdDAO = new GoogleDataprocDAO(dataprocConfig, proxyConfig, clusterResourcesConfig)
    val clusterMonitorSupervisor = system.actorOf(ClusterMonitorSupervisor.props(monitorConfig, gdDAO, dbRef))
    val leonardoService = new LeonardoService(dataprocConfig, clusterResourcesConfig, proxyConfig, gdDAO, dbRef, clusterMonitorSupervisor)
    val clusterDnsCache = system.actorOf(ClusterDnsCache.props(proxyConfig, dbRef))
    val proxyService = new ProxyService(proxyConfig, dbRef, clusterDnsCache)
    val leoRoutes = new LeoRoutes(leonardoService, proxyService, config.as[SwaggerConfig]("swagger"))

    startClusterMonitors(dbRef, clusterMonitorSupervisor)

    Http().bindAndHandle(leoRoutes.route, "0.0.0.0", 8080)
      .recover {
        case t: Throwable =>
          logger.error("FATAL - failure starting http server", t)
          throw t
      }
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
