package org.broadinstitute.dsde.workbench.leonardo
package monitor

import akka.actor.{Actor, Props, Timers}
import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.dsde.workbench.google.GoogleProjectDAO
import org.broadinstitute.dsde.workbench.leonardo.config.ClusterToolConfig
import org.broadinstitute.dsde.workbench.leonardo.dao.ToolDAO
import org.broadinstitute.dsde.workbench.leonardo.dao.google.GoogleDataprocDAO
import org.broadinstitute.dsde.workbench.leonardo.db.DbReference
import org.broadinstitute.dsde.workbench.leonardo.model.{Cluster, ClusterTool}
import org.broadinstitute.dsde.workbench.newrelic.NewRelicMetrics
import cats.implicits._
import org.broadinstitute.dsde.workbench.leonardo.model.ClusterTool.Welder
import ClusterToolMonitor._
import cats.effect.IO

import scala.concurrent.Future

object ClusterToolMonitor {

  def props(config: ClusterToolConfig,
            gdDAO: GoogleDataprocDAO,
            googleProjectDAO: GoogleProjectDAO,
            dbRef: DbReference,
            newRelic: NewRelicMetrics[IO])(implicit clusterToolToToolDao: ClusterTool => ToolDAO[ClusterTool]): Props =
    Props(new ClusterToolMonitor(config, gdDAO, googleProjectDAO, dbRef, newRelic))

  sealed trait ClusterToolMonitorMessage
  case object DetectClusterStatus extends ClusterToolMonitorMessage
  case object TimerKey extends ClusterToolMonitorMessage

  case class ToolStatus(val isUp: Boolean, val tool: ClusterTool, val cluster: Cluster)
}

/**
 * Monitors tool status (Jupyter, RStudio, Welder, etc) on Running clusters and reports if any tool is down.
 */
class ClusterToolMonitor(
  config: ClusterToolConfig,
  gdDAO: GoogleDataprocDAO,
  googleProjectDAO: GoogleProjectDAO,
  dbRef: DbReference,
  newRelic: NewRelicMetrics[IO]
)(implicit clusterToolToToolDao: ClusterTool => ToolDAO[ClusterTool])
    extends Actor
    with Timers
    with LazyLogging {

  import context._

  override def preStart(): Unit = {
    super.preStart()
    timers.startPeriodicTimer(TimerKey, DetectClusterStatus, config.pollPeriod)
  }

  override def receive: Receive = {
    case DetectClusterStatus =>
      for {
        activeClusters <- getActiveClustersFromDatabase
        statuses <- Future.traverse(activeClusters)(checkClusterStatus)
        //statuses is a Seq[Seq[ToolStatus]] because we create a Seq[ToolStatus] for each cluster, necessitating the flatten below
        _ <- Future.traverse(statuses.flatten)(handleClusterStatus)
      } yield ()
  }

  private def handleClusterStatus(status: ToolStatus): Future[Unit] =
    if (!status.isUp) {
      val toolName = status.tool.toString
      logger.warn(
        s"The tool ${toolName} is down on cluster ${status.cluster.googleProject.value}/${status.cluster.clusterName.value}"
      )
      newRelic.incrementCounterFuture(toolName + "Down")
    } else {
      Future.unit
    }

  private def getActiveClustersFromDatabase: Future[Seq[Cluster]] =
    dbRef.inTransaction {
      _.clusterQuery.listRunningOnly
    }

  def checkClusterStatus(cluster: Cluster): Future[Seq[ToolStatus]] =
    ClusterTool.values.toList.traverse {
      case tool =>
        tool
          .isProxyAvailable(cluster.googleProject, cluster.clusterName)
          .map(status => {
            //the if else is necessary because otherwise we will be reporting the metric 'welder down' on all clusters without welder, which is not the desired behavior
            //TODO: change to  `ToolStatus(status, tool, cluster)` when data syncing is fully rolled out
            if (!cluster.welderEnabled && tool == Welder) {
              ToolStatus(true, tool, cluster)
            } else {
              ToolStatus(status, tool, cluster)
            }
          })
    }

}
