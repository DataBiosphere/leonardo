package org.broadinstitute.dsde.workbench.leonardo
package monitor

import akka.actor.{Actor, Props, Timers}
import cats.effect.IO
import cats.effect.unsafe.implicits.global
import cats.syntax.all._
import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.dsde.workbench.leonardo.config.ClusterToolConfig
import org.broadinstitute.dsde.workbench.leonardo.dao.ToolDAO
import org.broadinstitute.dsde.workbench.leonardo.db.{clusterQuery, DbReference}
import org.broadinstitute.dsde.workbench.leonardo.monitor.ClusterToolMonitor._
import org.broadinstitute.dsde.workbench.openTelemetry.OpenTelemetryMetrics

import scala.concurrent.ExecutionContext

object ClusterToolMonitor {

  def props(
    config: ClusterToolConfig,
    dbRef: DbReference[IO],
    metrics: OpenTelemetryMetrics[IO]
  )(implicit clusterToolToToolDao: RuntimeContainerServiceType => ToolDAO[IO, RuntimeContainerServiceType],
    ec: ExecutionContext): Props =
    Props(new ClusterToolMonitor(config, dbRef, metrics))

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
  dbRef: DbReference[IO],
  metrics: OpenTelemetryMetrics[IO]
)(implicit clusterToolToToolDao: RuntimeContainerServiceType => ToolDAO[IO, RuntimeContainerServiceType],
  ec: ExecutionContext)
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
          s"The tool ${toolName} is down on runtime ${status.runtime.cloudContext.asStringWithProvider}/${status.runtime.runtimeName.asString}"
        )
      ) >> metrics.incrementCounter(toolName + "Down", 1)
    } else IO.unit

  private def getActiveClustersFromDatabase: IO[Seq[RunningRuntime]] =
    dbRef.inTransaction(
      clusterQuery.listRunningOnly
    )

  def checkClusterStatus(runtime: RunningRuntime): IO[List[ToolStatus]] =
    runtime.containers.traverse { tool =>
      tool
        .isProxyAvailable(runtime.cloudContext, runtime.runtimeName)
        .map(status => ToolStatus(status, tool, runtime))
    }
}
