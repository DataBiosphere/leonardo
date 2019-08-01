package org.broadinstitute.dsde.workbench.leonardo.monitor

import akka.actor.{Actor, Props, Timers}
import org.broadinstitute.dsde.workbench.leonardo.Metrics
import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.dsde.workbench.google.GoogleProjectDAO
import org.broadinstitute.dsde.workbench.leonardo.config.ClusterServiceConfig
import org.broadinstitute.dsde.workbench.leonardo.dao.{JupyterDAO, WelderDAO}
import org.broadinstitute.dsde.workbench.leonardo.dao.google.GoogleDataprocDAO
import org.broadinstitute.dsde.workbench.leonardo.db.DbReference
import org.broadinstitute.dsde.workbench.leonardo.model.Cluster
import org.broadinstitute.dsde.workbench.leonardo.monitor.ClusterServiceMonitor.{DetectClusterStatus, JupyterStatus, Status, TimerKey, WelderStatus}
import org.broadinstitute.dsde.workbench.model.google.GoogleProject
import org.broadinstitute.dsde.workbench.newrelic.NewRelicMetrics

import scala.concurrent.Future

object ClusterServiceMonitor {


  val newRelic: NewRelicMetrics = Metrics.newRelic

  def props(config: ClusterServiceConfig, gdDAO: GoogleDataprocDAO, googleProjectDAO: GoogleProjectDAO, dbRef: DbReference, welderDAO: WelderDAO, jupyterDAO: JupyterDAO, newRelic: NewRelicMetrics): Props = {
    Props(new ClusterServiceMonitor(config, gdDAO, googleProjectDAO, dbRef, welderDAO, jupyterDAO, newRelic: NewRelicMetrics))
  }

  sealed trait ClusterServiceMonitorMessage
  case object DetectClusterStatus extends ClusterServiceMonitorMessage
  case object TimerKey extends ClusterServiceMonitorMessage

  sealed trait ServiceStatus extends Product with Serializable
  sealed trait JupyterStatus extends ServiceStatus with Product with Serializable
  object JupyterStatus {
    final case object JupyterOK extends JupyterStatus
    final case object JupyterDown extends JupyterStatus
  }

  sealed trait WelderStatus extends ServiceStatus with Product with Serializable
  object WelderStatus {
    final case object WelderOK extends WelderStatus
    final case object WelderDown extends WelderStatus
  }

  case class Status(val welderStatus: WelderStatus, val jupyterStatus: JupyterStatus)
}

/**
  * This monitor periodically sweeps the Leo database and checks for clusters which no longer exist in Google.
  */
class ClusterServiceMonitor(config: ClusterServiceConfig, gdDAO: GoogleDataprocDAO, googleProjectDAO: GoogleProjectDAO, dbRef: DbReference, welderDAO: WelderDAO, jupyterDAO: JupyterDAO, newRelic: NewRelicMetrics) extends Actor with Timers with LazyLogging {

  import context._

  override def preStart(): Unit = {
    super.preStart()
    timers.startPeriodicTimer(TimerKey, DetectClusterStatus, config.pollPeriod)
  }

  override def receive: Receive = {
    case DetectClusterStatus =>
      // Get active clusters from the Leo DB, grouped by project
      val activeClusters: Future[List[Cluster]] = getActiveClustersFromDatabase.flatMap(clusterMap => {
        Future.successful(clusterMap.values.flatten.toList)
      })

      //check the status of the workers in each active cluster and log instances of a down worker to new relic
      activeClusters.foreach { cs =>
        cs.foreach { cluster =>
          checkClusterStatus(cluster).foreach { status =>

            if (status.jupyterStatus.equals(JupyterStatus.JupyterDown)) {
              logger.info("jupyter down")
              newRelic.incrementCounterIO("jupyterDown").unsafeRunAsync(_ => ())
            }

            if (status.welderStatus.equals(WelderStatus.WelderDown)) {
              logger.info("welder enabled and down")
              newRelic.incrementCounterIO("welderDown").unsafeRunAsync(_ => ())
            }

          ()
          }
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

  def checkClusterStatus(cluster: Cluster): Future[Status] = {
    for {
      isWelderUp <- welderDAO.isProxyAvailable(cluster.googleProject, cluster.clusterName)
      isJupyterUp <- jupyterDAO.isProxyAvailable(cluster.googleProject, cluster.clusterName)

      //if welder isn't enabled, the status is will be OK
      welderStatus: WelderStatus = if (!isWelderUp && cluster.welderEnabled) WelderStatus.WelderDown else WelderStatus.WelderOK
      jupyterStatus: JupyterStatus = if (isJupyterUp) JupyterStatus.JupyterOK else JupyterStatus.JupyterDown
    } yield Status(welderStatus, jupyterStatus)
  }
}
