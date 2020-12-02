package org.broadinstitute.dsde.workbench.leonardo.monitor

import java.time.Instant

import cats.data.Chain
import cats.effect.{Concurrent, ContextShift, Timer}
import cats.implicits._
import cats.{Order, Parallel}
import fs2.Stream
import fs2.concurrent.InspectableQueue
import org.broadinstitute.dsde.workbench.leonardo.db._
import org.broadinstitute.dsde.workbench.leonardo.http._
import org.broadinstitute.dsde.workbench.leonardo.monitor.DateAccessedUpdater._
import org.broadinstitute.dsde.workbench.leonardo.{AppName, RuntimeName}
import org.broadinstitute.dsde.workbench.model.google.GoogleProject
import org.broadinstitute.dsde.workbench.openTelemetry.OpenTelemetryMetrics
import slick.dbio.DBIO

import scala.concurrent.ExecutionContext
import scala.concurrent.duration.FiniteDuration

class DateAccessedUpdater[F[_]: ContextShift: Timer: Parallel](
  config: DateAccessedUpdaterConfig,
  queue: InspectableQueue[F, UpdateDateAccessedMessage]
)(implicit F: Concurrent[F], metrics: OpenTelemetryMetrics[F], dbRef: DbReference[F], ec: ExecutionContext) {

  val process: Stream[F, Unit] =
    (Stream.sleep[F](config.interval) ++ Stream.eval(check)).repeat

  private[monitor] val check: F[Unit] =
    queue.tryDequeueChunk1(config.maxUpdate).flatMap { chunks =>
      chunks
        .traverse(c =>
          messagesToUpdate(c.toChain).traverseN {
            case (runtime, app) =>
              List(updateRuntimeDateAccessed(runtime), updateAppDateAccessed(app)).parSequence_
          }
        )
        .void
    }

  private def updateRuntimeDateAccessed(msg: UpdateDateAccessedMessage.Runtime): F[Unit] =
    metrics.incrementCounter("jupyterAccessCount") >>
      (clusterQuery.clearKernelFoundBusyDateByProjectAndName(msg.googleProject, msg.runtimeName, msg.dateAccessed) >>
        clusterQuery.updateDateAccessedByProjectAndName(msg.googleProject, msg.runtimeName, msg.dateAccessed)).transaction.void

  private def updateAppDateAccessed(msg: UpdateDateAccessedMessage.App): F[Unit] =
    metrics.incrementCounter("appAccessCount") >>
      appQuery
        .getAppIdByProjectAndName(msg.googleProject, msg.appName)
        .flatMap {
          case Some(appId) =>
            appQuery.clearFoundBusyDate(appId) >> appQuery.updateDateAccessed(appId, msg.dateAccessed)
          case None => DBIO.successful(0)
        }
        .transaction
        .void
}

object DateAccessedUpdater {
  implicit val runtimeMessageOrder: Order[UpdateDateAccessedMessage.Runtime] =
    Order.fromLessThan[UpdateDateAccessedMessage.Runtime] { (msg1, msg2) =>
      if (msg1.googleProject == msg2.googleProject && msg1.runtimeName == msg2.runtimeName)
        msg1.dateAccessed.toEpochMilli < msg2.dateAccessed.toEpochMilli
      else
        false //we don't really care about order if they're not the same runtime, but we just need an Order if they're the same
    }

  implicit val appMessageOrder: Order[UpdateDateAccessedMessage.App] =
    Order.fromLessThan[UpdateDateAccessedMessage.App] { (msg1, msg2) =>
      if (msg1.googleProject == msg2.googleProject && msg1.appName == msg2.appName)
        msg1.dateAccessed.toEpochMilli < msg2.dateAccessed.toEpochMilli
      else
        false //we don't really care about order if they're not the same app, but we just need an Order if they're the same
    }

  def messagesToUpdate(
    messages: Chain[UpdateDateAccessedMessage]
  ): (List[UpdateDateAccessedMessage.Runtime], List[UpdateDateAccessedMessage.App]) =
    messages
      .partitionEither {
        case r: UpdateDateAccessedMessage.Runtime => Left(r)
        case a: UpdateDateAccessedMessage.App     => Right(a)
      }
      .bimap(
        lastOfGroup((m: UpdateDateAccessedMessage.Runtime) => s"${m.googleProject.value}/${m.runtimeName.asString}"),
        lastOfGroup((m: UpdateDateAccessedMessage.App) => s"${m.googleProject.value}/${m.appName.value}")
      )

  // group messages, and discard all older messages for the same resource
  private def lastOfGroup[A: Order](f: A => String)(as: Chain[A]): List[A] =
    as.groupBy(f)
      .toList
      .traverse {
        case (_, as) =>
          as.toList.sorted.lastOption
      }
      .getOrElse(List.empty)
}

sealed trait UpdateDateAccessedMessage extends Product with Serializable {
  def dateAccessed: Instant
}
object UpdateDateAccessedMessage {
  final case class Runtime(runtimeName: RuntimeName, googleProject: GoogleProject, dateAccessed: Instant)
      extends UpdateDateAccessedMessage
  final case class App(appName: AppName, googleProject: GoogleProject, dateAccessed: Instant)
      extends UpdateDateAccessedMessage

}
final case class DateAccessedUpdaterConfig(interval: FiniteDuration, maxUpdate: Int, queueSize: Int)
