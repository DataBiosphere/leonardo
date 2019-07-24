package org.broadinstitute.dsde.workbench.leonardo.monitor

import akka.actor.{Actor, Props, Timers}
import cats.effect.IO
import cats.implicits._
import org.broadinstitute.dsde.workbench.leonardo.Metrics
import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.dsde.workbench.google.GoogleProjectDAO
import org.broadinstitute.dsde.workbench.leonardo.config.ClusterServiceConfig
import org.broadinstitute.dsde.workbench.leonardo.dao.{HttpJupyterDAO, HttpWelderDAO}
import org.broadinstitute.dsde.workbench.leonardo.dao.google.GoogleDataprocDAO
import org.broadinstitute.dsde.workbench.leonardo.db.DbReference
import org.broadinstitute.dsde.workbench.leonardo.model.Cluster
import org.broadinstitute.dsde.workbench.leonardo.monitor.ClusterServiceMonitor.{DetectClusterStatus, JupyterStatus, Status, TimerKey, WelderStatus}
import org.broadinstitute.dsde.workbench.model.google.GoogleProject

import scala.concurrent.Future

object ClusterServiceMonitor {

  def props(config: ClusterServiceConfig, gdDAO: GoogleDataprocDAO, googleProjectDAO: GoogleProjectDAO, dbRef: DbReference, welderDAO: HttpWelderDAO, jupyterDAO: HttpJupyterDAO): Props = {
    Props(new ClusterServiceMonitor(config, gdDAO, googleProjectDAO, dbRef, welderDAO, jupyterDAO))
  }

  sealed trait ClusterServiceMonitorMessage
  case object DetectClusterStatus extends ClusterServiceMonitorMessage
  case object TimerKey extends ClusterServiceMonitorMessage

  sealed trait ServiceStatus extends Product with Serializable
  sealed trait JupyterStatus extends ServiceStatus with Product with Serializable
  object JupyterStatus {
    final case object JupyterOK extends ServiceStatus
    final case object JupyterDown extends ServiceStatus
  }

  sealed trait WelderStatus extends ServiceStatus with Product with Serializable
  object WelderStatus {
    final case object WelderOK extends ServiceStatus
    final case object WelderDown extends ServiceStatus
  }

  case class Status(val welderStatus: WelderStatus, val jupyterStatus: JupyterStatus)
}

/**
  * This monitor periodically sweeps the Leo database and checks for clusters which no longer exist in Google.
  */
class ClusterServiceMonitor(config: ClusterServiceConfig, gdDAO: GoogleDataprocDAO, googleProjectDAO: GoogleProjectDAO, dbRef: DbReference, welderDAO: HttpWelderDAO, jupyterDAO: HttpJupyterDAO) extends Actor with Timers with LazyLogging {

  import context._

  override def preStart(): Unit = {
    super.preStart()
    timers.startPeriodicTimer(TimerKey, DetectClusterStatus, config.pollPeriod)
  }

  override def receive: Receive = {
    case DetectClusterStatus =>
      // Get active clusters from the Leo DB, grouped by project
      val activeClusters: Future[List[Cluster]] = getActiveClustersFromDatabase.flatMap(clusterMap => {
        logger.info("active clusters: " + clusterMap.values.flatten.toString())
        Future.successful(clusterMap.values.flatten.toList)
      })

      //check the status of the workers in each active cluster and log instances of a down worker to new relic
      activeClusters.flatMap { cs =>
        cs.traverse { cluster =>
          checkClusterStatus(cluster).map(status =>
            for {
              _ <- if (status.jupyterStatus.equals(JupyterStatus.JupyterDown)) Metrics.newRelic.incrementCounterIO("jupyterDown") else IO.unit
              _ <- if (status.welderStatus.equals(WelderStatus.WelderDown)) Metrics.newRelic.incrementCounterIO("welderDown") else IO.unit
            } yield (logger.info("status for cluster: " + status.toString)))
        }
      }
  }

  private def getActiveClustersFromDatabase: Future[Map[GoogleProject, Seq[Cluster]]] = {
    dbRef.inTransaction {
      _.clusterQuery.listActive
    } map { clusters =>
      clusters.groupBy(_.googleProject)
    }
  }

  private def checkClusterStatus(cluster: Cluster): Future[Status] = {
    logger.info(s"Deleting zombie cluster: ${cluster.projectNameString}")
    for {
      welderStatus <- welderDAO.isProxyAvailable(cluster.googleProject, cluster.clusterName)
      jupyterStatus <- jupyterDAO.isProxyAvailable(cluster.googleProject, cluster.clusterName)
      status = Status(welderStatus.asInstanceOf[WelderStatus], jupyterStatus.asInstanceOf[JupyterStatus])
    } yield status
  }
}
