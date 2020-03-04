package org.broadinstitute.dsde.workbench.leonardo
package monitor

import akka.actor.{Actor, Props, Timers}
import cats.effect.{ContextShift, IO}
import cats.implicits._
import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.dsde.workbench.google.GoogleProjectDAO
import org.broadinstitute.dsde.workbench.leonardo.config.ClusterToolConfig
import org.broadinstitute.dsde.workbench.leonardo.dao.ToolDAO
import org.broadinstitute.dsde.workbench.leonardo.dao.google.GoogleDataprocDAO
import org.broadinstitute.dsde.workbench.leonardo.db.{clusterQuery, DbReference}
import org.broadinstitute.dsde.workbench.leonardo.monitor.ClusterToolMonitor._
import org.broadinstitute.dsde.workbench.newrelic.NewRelicMetrics

import scala.concurrent.ExecutionContext

object ClusterToolMonitor {

  def props(
    config: ClusterToolConfig,
    gdDAO: GoogleDataprocDAO,
    googleProjectDAO: GoogleProjectDAO,
    dbRef: DbReference[IO],
    newRelic: NewRelicMetrics[IO]
  )(implicit clusterToolToToolDao: RuntimeContainerServiceType => ToolDAO[RuntimeContainerServiceType],
    ec: ExecutionContext,
    cs: ContextShift[IO]): Props =
    Props(new ClusterToolMonitor(config, gdDAO, googleProjectDAO, dbRef, newRelic))

  sealed trait ClusterToolMonitorMessage
  case object DetectClusterStatus extends ClusterToolMonitorMessage
  case object TimerKey extends ClusterToolMonitorMessage

  final case class ToolStatus(isUp: Boolean, tool: RuntimeContainerServiceType, runtime: RunningRuntime)
}

/**
 * Monitors tool status (Jupyter, RStudio, Welder, etc) on Running clusters and reports if any tool is down.
 */
class ClusterToolMonitor(
  config: ClusterToolConfig,
  gdDAO: GoogleDataprocDAO,
  googleProjectDAO: GoogleProjectDAO,
  dbRef: DbReference[IO],
  newRelic: NewRelicMetrics[IO]
)(implicit clusterToolToToolDao: RuntimeContainerServiceType => ToolDAO[RuntimeContainerServiceType],
  ec: ExecutionContext,
  cs: ContextShift[IO])
    extends Actor
    with Timers
    with LazyLogging {

  override def preStart(): Unit = {
    super.preStart()
    timers.startTimerWithFixedDelay(TimerKey, DetectClusterStatus, config.pollPeriod)
  }

  override def receive: Receive = {
    case DetectClusterStatus =>
      val res = for {
        activeClusters <- getActiveClustersFromDatabase
        statuses <- activeClusters.toList.parFlatTraverse(checkClusterStatus)
        //statuses is a Seq[Seq[ToolStatus]] because we create a Seq[ToolStatus] for each cluster, necessitating the flatten below
        _ <- statuses.parTraverse(handleClusterStatus)
      } yield ()
      res.unsafeToFuture()
    case e => logger.warn(s"Unexpected message ${e}")
  }

  private def handleClusterStatus(status: ToolStatus): IO[Unit] =
    if (!status.isUp) {
      val toolName = status.tool.toString
      IO(
        logger.warn(
          s"The tool ${toolName} is down on cluster ${status.runtime.googleProject.value}/${status.runtime.runtimeName.asString}"
        )
      ) >> newRelic.incrementCounter(toolName + "Down")
    } else IO.unit

  private def getActiveClustersFromDatabase: IO[Seq[RunningRuntime]] =
    dbRef.inTransaction(
      clusterQuery.listRunningOnly
    )

  def checkClusterStatus(runtime: RunningRuntime): IO[List[ToolStatus]] =
    runtime.containers.traverse { tool =>
      tool
        .isProxyAvailable(runtime.googleProject, runtime.runtimeName)
        .map { status =>
          ToolStatus(status, tool, runtime)
        }
    }
}
