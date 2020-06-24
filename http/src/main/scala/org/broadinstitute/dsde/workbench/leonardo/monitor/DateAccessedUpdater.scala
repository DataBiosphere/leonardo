package org.broadinstitute.dsde.workbench.leonardo.monitor

import java.time.Instant

import cats.Order
import cats.effect.{Concurrent, ContextShift, Timer}
import cats.implicits._
import fs2.Stream
import fs2.concurrent.InspectableQueue
import org.broadinstitute.dsde.workbench.leonardo.RuntimeName
import org.broadinstitute.dsde.workbench.leonardo.db._
import org.broadinstitute.dsde.workbench.leonardo.http._
import org.broadinstitute.dsde.workbench.model.google.GoogleProject
import org.broadinstitute.dsde.workbench.openTelemetry.OpenTelemetryMetrics
import DateAccessedUpdater._
import cats.data.Chain
import io.chrisdavenport.log4cats.Logger

import scala.concurrent.ExecutionContext
import scala.concurrent.duration.FiniteDuration

class DateAccessedUpdater[F[_]: ContextShift: Timer](
  config: DateAccessedUpdaterConfig,
  queue: InspectableQueue[F, UpdateDateAccessMessage]
)(implicit F: Concurrent[F],
  metrics: OpenTelemetryMetrics[F],
  dbRef: DbReference[F],
  ec: ExecutionContext,
  logger: Logger[F]) {

  val process: Stream[F, Unit] =
    (Stream.sleep[F](config.interval) ++ Stream.eval(check)).repeat

  private[monitor] val check: F[Unit] =
    logger.info(s"Going to update dateAccessed") >> queue.tryDequeueChunk1(config.maxUpdate).flatMap { chunks =>
      chunks
        .traverse(c =>
          messagesToUpdate(c.toChain)
            .traverse(updateDateAccessed)
        )
        .void
    }

  private def updateDateAccessed(msg: UpdateDateAccessMessage): F[Unit] =
    metrics.incrementCounter("jupyterAccessCount") >>
      clusterQuery
        .clearKernelFoundBusyDateByProjectAndName(msg.googleProject, msg.runtimeName, msg.dateAccessd)
        .flatMap(_ =>
          clusterQuery.updateDateAccessedByProjectAndName(msg.googleProject, msg.runtimeName, msg.dateAccessd)
        )
        .transaction
        .void
}

object DateAccessedUpdater {
  implicit val updateDateAccessMessageOrder: Order[UpdateDateAccessMessage] =
    Order.fromLessThan[UpdateDateAccessMessage] { (msg1, msg2) =>
      if (msg1.googleProject == msg2.googleProject && msg1.runtimeName == msg2.runtimeName)
        msg1.dateAccessd.toEpochMilli < msg2.dateAccessd.toEpochMilli
      else
        false //we don't really care about order if they're not the same runtime, but we just need an Order if they're the same
    }

  // group all messages by googleProject and runtimeName, and discard all older messages for the same runtime
  def messagesToUpdate(messages: Chain[UpdateDateAccessMessage]): List[UpdateDateAccessMessage] = {
    messages.groupBy(m => s"${m.runtimeName.asString}/${m.googleProject.value}").toList.traverse {
      case (_, messages) =>
        messages.toChain.toList.sorted.lastOption
    }
  }.getOrElse(List.empty)
}

final case class DateAccessedUpdaterConfig(interval: FiniteDuration, maxUpdate: Int, queueSize: Int)
final case class UpdateDateAccessMessage(runtimeName: RuntimeName, googleProject: GoogleProject, dateAccessd: Instant) {
  override def toString: String =
    s"Message: ${googleProject.value}/${runtimeName.asString}, ${dateAccessd.toEpochMilli}"
}
