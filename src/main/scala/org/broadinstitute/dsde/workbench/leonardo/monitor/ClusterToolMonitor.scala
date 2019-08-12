package org.broadinstitute.dsde.workbench.leonardo.monitor

import akka.actor.{Actor, Props, Timers}
import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.dsde.workbench.google.GoogleProjectDAO
import org.broadinstitute.dsde.workbench.leonardo.config.ClusterServiceConfig
import org.broadinstitute.dsde.workbench.leonardo.dao.ToolDAO
import org.broadinstitute.dsde.workbench.leonardo.dao.google.GoogleDataprocDAO
import org.broadinstitute.dsde.workbench.leonardo.db.DbReference
import org.broadinstitute.dsde.workbench.leonardo.model.{Cluster, ClusterTool}
import org.broadinstitute.dsde.workbench.leonardo.monitor.ClusterToolMonitor.{DetectClusterStatus, TimerKey, ToolStatus}
import org.broadinstitute.dsde.workbench.newrelic.NewRelicMetrics
import cats.implicits._
import org.broadinstitute.dsde.workbench.leonardo.model.ClusterTool.Welder

import scala.concurrent.Future

object ClusterToolMonitor {

  def props(config: ClusterServiceConfig, gdDAO: GoogleDataprocDAO, googleProjectDAO: GoogleProjectDAO, dbRef: DbReference, toolDAOs: Map[ClusterTool, ToolDAO], newRelic: NewRelicMetrics): Props = {
    Props(new ClusterToolMonitor(config, gdDAO, googleProjectDAO, dbRef, toolDAOs, newRelic))
  }

  sealed trait ClusterServiceMonitorMessage
  case object DetectClusterStatus extends ClusterServiceMonitorMessage
  case object TimerKey extends ClusterServiceMonitorMessage

  case class ToolStatus(val isUp: Boolean, val tool: ClusterTool)
}

/**
  * This monitor periodically sweeps the Leo database and checks for clusters which no longer exist in Google.
  */
class ClusterToolMonitor(config: ClusterServiceConfig, gdDAO: GoogleDataprocDAO, googleProjectDAO: GoogleProjectDAO, dbRef: DbReference, toolDAOs: Map[ClusterTool, ToolDAO], newRelic: NewRelicMetrics) extends Actor with Timers with LazyLogging {

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
        _ <- Future.traverse(statuses)(handleClusterStatus)
      } yield ()
  }

  private def handleClusterStatus(statuses: List[ToolStatus]): Future[Unit] = {
    statuses.foreach { status =>
      if (!status.isUp) {
        val toolName = status.tool.toString
        logger.info(s"The tool ${toolName} is down")
        newRelic.incrementCounterIO(toolName + "Down")
      }
    }
    Future.unit
  }

  private def getActiveClustersFromDatabase: Future[Seq[Cluster]] = {
    dbRef.inTransaction {
      _.clusterQuery.listActiveOnly
    }
  }

  def checkClusterStatus(cluster: Cluster): Future[List[ToolStatus]] = {
    toolDAOs.toList.traverse { case (tool, dao) =>
      dao
        .isProxyAvailable(cluster.googleProject, cluster.clusterName)
        .map(status => {
          //the if else is necessary because otherwise we will be reporting the metric 'welder down' on all clusters without welder, which is not the desired behavior
          //TODO: change to  `ToolStatus(status, tool)` when welder is gone
          if (!cluster.welderEnabled && tool == Welder) {  ToolStatus(true, tool)  }
          else { ToolStatus(status, tool) }
        })
    }
  }

}
