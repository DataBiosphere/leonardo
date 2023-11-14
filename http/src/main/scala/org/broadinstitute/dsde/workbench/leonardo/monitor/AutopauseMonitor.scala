package org.broadinstitute.dsde.workbench.leonardo
package monitor

import fs2.Stream
import cats.effect.Async
import cats.effect.std.Queue
import cats.syntax.all._
import org.typelevel.log4cats.StructuredLogger
import org.broadinstitute.dsde.workbench.leonardo.config.AutoFreezeConfig
import org.broadinstitute.dsde.workbench.leonardo.dao.JupyterDAO
import org.broadinstitute.dsde.workbench.leonardo.db._
import org.broadinstitute.dsde.workbench.leonardo.http.dbioToIO
import org.broadinstitute.dsde.workbench.model.TraceId
import org.broadinstitute.dsde.workbench.openTelemetry.OpenTelemetryMetrics
import cats.Show
import java.time.Instant
import scala.concurrent.ExecutionContext

/**
 * This monitor periodically sweeps the Leo database and auto pause clusters that have been running for too long.
 */
class AutopauseMonitor[F[_]](
  config: AutoFreezeConfig,
  jupyterDAO: JupyterDAO[F],
  publisherQueue: Queue[F, LeoPubsubMessage]
)(implicit
  dbRef: DbReference[F],
  ec: ExecutionContext
) extends BackgroundProcess[F, RuntimeToAutoPause] {
  override def name: String =
    "autopause" // autopauseRuntime is more accurate. But keep the current name so that existing metrics won't break
  override def interval: scala.concurrent.duration.FiniteDuration = config.autoFreezeCheckInterval

  def isAutopauseable(cluster: RuntimeToAutoPause, now: Instant)(implicit
    F: Async[F],
    metrics: OpenTelemetryMetrics[F],
    logger: StructuredLogger[F]
  ): F[Boolean] =
    jupyterDAO.isAllKernelsIdle(cluster.cloudContext, cluster.runtimeName).attempt.flatMap {
      case Left(t) =>
        logger.error(s"Fail to get kernel status for ${cluster.projectNameString} due to $t").as(true)
      case Right(isIdle) =>
        if (!isIdle) {
          val maxKernelActiveTimeExceeded = cluster.kernelFoundBusyDate match {
            case Some(attemptedDate) =>
              val maxBusyLimitReached =
                now.toEpochMilli - attemptedDate.toEpochMilli > config.maxKernelBusyLimit.toMillis
              F.pure(maxBusyLimitReached)
            case None =>
              clusterQuery
                .updateKernelFoundBusyDate(cluster.id, now, now)
                .transaction
                .as(false) // max kernel active time has not been exceeded
          }

          maxKernelActiveTimeExceeded.ifM(
            metrics.incrementCounter("autoPause/maxKernelActiveTimeExceeded") >>
              logger
                .info(
                  s"Auto pausing ${cluster.cloudContext}/${cluster.runtimeName} due to exceeded max kernel active time"
                )
                .as(true),
            metrics.incrementCounter("autoPause/activeKernelClusters") >>
              logger
                .info(
                  s"Not going to auto pause cluster ${cluster.cloudContext}/${cluster.runtimeName} due to active kernels"
                )
                .as(false)
          )
        } else F.pure(isIdle)
    }

  override def getCandidates(now: Instant)(implicit
    F: Async[F],
    metrics: OpenTelemetryMetrics[F],
    logger: StructuredLogger[F]
  ): F[Seq[RuntimeToAutoPause]] = for {
    candidates <- clusterQuery.getClustersReadyToAutoFreeze.transaction
    filtered <- candidates.toList.filterA { a =>
      isAutopauseable(a, now)
    }
  } yield filtered

  override def doAction(a: RuntimeToAutoPause, traceId: TraceId, now: Instant)(implicit F: Async[F]): F[Unit] =
    for {
      _ <- clusterQuery.updateClusterStatus(a.id, RuntimeStatus.PreStopping, now).transaction
      _ <- publisherQueue.offer(LeoPubsubMessage.StopRuntimeMessage(a.id, Some(traceId)))
    } yield ()
}

object AutopauseMonitor {
  def process[F[_]](config: AutoFreezeConfig, jupyterDAO: JupyterDAO[F], publisherQueue: Queue[F, LeoPubsubMessage])(
    implicit
    dbRef: DbReference[F],
    ec: ExecutionContext,
    F: Async[F],
    openTelemetry: OpenTelemetryMetrics[F],
    logger: StructuredLogger[F]
  ): Stream[F, Unit] = {
    val autopauseMonitor = apply(config, jupyterDAO, publisherQueue)

    implicit val runtimeToAutoPauseShowInstance: Show[RuntimeToAutoPause] =
      Show[RuntimeToAutoPause](runtimeToAutoPause => runtimeToAutoPause.projectNameString)
    autopauseMonitor.process
  }

  private def apply[F[_]](config: AutoFreezeConfig,
                          jupyterDAO: JupyterDAO[F],
                          publisherQueue: Queue[F, LeoPubsubMessage]
  )(implicit
    dbRef: DbReference[F],
    ec: ExecutionContext
  ): AutopauseMonitor[F] =
    new AutopauseMonitor(config, jupyterDAO, publisherQueue)

}

final case class RuntimeToAutoPause(id: Long,
                                    runtimeName: RuntimeName,
                                    cloudContext: CloudContext,
                                    kernelFoundBusyDate: Option[Instant]
) {
  def projectNameString: String = s"${cloudContext.asStringWithProvider}/${runtimeName.asString}"
}
