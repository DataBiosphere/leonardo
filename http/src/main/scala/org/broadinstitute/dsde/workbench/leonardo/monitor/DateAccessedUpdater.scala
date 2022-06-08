package org.broadinstitute.dsde.workbench.leonardo.monitor

import cats.data.Chain
import cats.effect.Async
import cats.effect.std.Queue
import cats.syntax.all._
import fs2.Stream
import org.broadinstitute.dsde.workbench.leonardo.db._
import org.broadinstitute.dsde.workbench.leonardo.http._
import org.broadinstitute.dsde.workbench.leonardo.monitor.DateAccessedUpdater._
import org.broadinstitute.dsde.workbench.leonardo.{CloudContext, RuntimeName}
import org.broadinstitute.dsde.workbench.openTelemetry.OpenTelemetryMetrics
import org.typelevel.log4cats.Logger

import java.time.Instant
import scala.concurrent.ExecutionContext
import scala.concurrent.duration.FiniteDuration

class DateAccessedUpdater[F[_]](
  config: DateAccessedUpdaterConfig,
  queue: Queue[F, UpdateDateAccessMessage]
)(implicit
  F: Async[F],
  metrics: OpenTelemetryMetrics[F],
  dbRef: DbReference[F],
  logger: Logger[F],
  ec: ExecutionContext
) {

  val process: Stream[F, Unit] =
    (Stream.sleep[F](config.interval) ++ Stream.eval(
      check
        .handleErrorWith(e => logger.error(e)("Unexpected error occurred during DateAccessedUpdater monitoring"))
    )).repeat

  private[monitor] val check: F[Unit] =
    Stream
      .fromQueueUnterminated(queue)
      .chunkLimit(config.maxUpdate)
      .evalMap { chunk =>
        messagesToUpdate(chunk.toChain)
          .traverse(updateDateAccessed)
          .void
      }
      .compile
      .drain

  private def updateDateAccessed(msg: UpdateDateAccessMessage): F[Unit] =
    metrics.incrementCounter("jupyterAccessCount") >>
      clusterQuery
        .clearKernelFoundBusyDateByProjectAndName(msg.cloudContext, msg.runtimeName, msg.dateAccessd)
        .flatMap(_ =>
          clusterQuery.updateDateAccessedByProjectAndName(msg.cloudContext, msg.runtimeName, msg.dateAccessd)
        )
        .transaction
        .void
}

object DateAccessedUpdater {
  implicit val updateDateAccessMessageOrder: Ordering[UpdateDateAccessMessage] =
    Ordering.fromLessThan[UpdateDateAccessMessage] { (msg1, msg2) =>
      if (msg1.cloudContext == msg2.cloudContext && msg1.runtimeName == msg2.runtimeName)
        msg1.dateAccessd.toEpochMilli < msg2.dateAccessd.toEpochMilli
      else
        false // we don't really care about order if they're not the same runtime, but we just need an Order if they're the same
    }

  // group all messages by cloudContext and runtimeName, and discard all older messages for the same runtime
  def messagesToUpdate(messages: Chain[UpdateDateAccessMessage]): List[UpdateDateAccessMessage] =
    messages.groupBy(m => s"${m.runtimeName.asString}/${m.cloudContext.asStringWithProvider}").toList.traverse {
      case (_, messages) =>
        messages.toChain.toList.sorted.lastOption
    }
      .getOrElse(List.empty)
}

final case class DateAccessedUpdaterConfig(interval: FiniteDuration, maxUpdate: Int, queueSize: Int)
final case class UpdateDateAccessMessage(runtimeName: RuntimeName, cloudContext: CloudContext, dateAccessd: Instant) {
  override def toString: String =
    s"Message: ${cloudContext.asStringWithProvider}/${runtimeName.asString}, ${dateAccessd.toEpochMilli}"
}
