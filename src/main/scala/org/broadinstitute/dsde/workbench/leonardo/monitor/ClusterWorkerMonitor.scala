package org.broadinstitute.dsde.workbench.leonardo.monitor

import akka.actor.{Actor, Props, Timers}
import cats.effect.IO
import cats.implicits._
import org.broadinstitute.dsde.workbench.leonardo.Metrics
import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.dsde.workbench.google.GoogleProjectDAO
import org.broadinstitute.dsde.workbench.google2.GoogleStorageService
import org.broadinstitute.dsde.workbench.leonardo.config.ClusterWorkerMonitorConfig
import org.broadinstitute.dsde.workbench.leonardo.dao.{HttpJupyterDAO, HttpWelderDAO}
import org.broadinstitute.dsde.workbench.leonardo.dao.google.GoogleDataprocDAO
import org.broadinstitute.dsde.workbench.leonardo.db.DbReference
import org.broadinstitute.dsde.workbench.leonardo.model.Cluster
import org.broadinstitute.dsde.workbench.leonardo.monitor.ClusterWorkerMonitor.{DetectClusterWorkers, JupyterStatus, Status, TimerKey, WelderStatus}
import org.broadinstitute.dsde.workbench.model.google.GoogleProject

import scala.concurrent.Future

object ClusterWorkerMonitor {

  def props(config: ClusterWorkerMonitorConfig, gdDAO: GoogleDataprocDAO, googleProjectDAO: GoogleProjectDAO, dbRef: DbReference, welderDAO: HttpWelderDAO, jupyterDAO: HttpJupyterDAO): Props = {
    Props(new ClusterWorkerMonitor(config, gdDAO, googleProjectDAO, dbRef, welderDAO, jupyterDAO))
  }

  sealed trait ClusterWorkerMonitorMessage
  case object DetectClusterWorkers extends ClusterWorkerMonitorMessage
  case object TimerKey extends ClusterWorkerMonitorMessage

  sealed trait WorkerStatus extends Product with Serializable
  sealed trait JupyterStatus extends WorkerStatus with Product with Serializable
  object JupyterStatus {
    final case object JupyterOK extends WorkerStatus
    final case object JupyterDown extends WorkerStatus
  }

  sealed trait WelderStatus extends WorkerStatus with Product with Serializable
  object WelderStatus {
    final case object WelderOK extends WorkerStatus
    final case object WelderDown extends WorkerStatus
  }

  case class Status(val welderStatus: WelderStatus, val jupyterStatus: JupyterStatus)
}

/**
  * This monitor periodically sweeps the Leo database and checks for clusters which no longer exist in Google.
  */
class ClusterWorkerMonitor(config: ClusterWorkerMonitorConfig, gdDAO: GoogleDataprocDAO, googleProjectDAO: GoogleProjectDAO, dbRef: DbReference, welderDAO: HttpWelderDAO, jupyterDAO: HttpJupyterDAO) extends Actor with Timers with LazyLogging {

  import context._

  override def preStart(): Unit = {
    super.preStart()
    timers.startPeriodicTimer(TimerKey, DetectClusterWorkers, config.workerCheckPeriod)
  }

  override def receive: Receive = {
    case DetectClusterWorkers =>
      // Get active clusters from the Leo DB, grouped by project
      val activeClusters: Future[List[Cluster]] = getActiveClustersFromDatabase.flatMap(clusterMap => {
        Future.successful(clusterMap.values.flatten.toList)
      })

      //check the status of the workers in each active cluster
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
