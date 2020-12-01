package org.broadinstitute.dsde.workbench.leonardo.monitor

import java.time.Instant

import cats.Order
import cats.data.Chain
import cats.effect.{Concurrent, ContextShift, Timer}
import cats.implicits._
import fs2.Stream
import fs2.concurrent.InspectableQueue
import org.broadinstitute.dsde.workbench.leonardo.{AppName, RuntimeName}
import org.broadinstitute.dsde.workbench.leonardo.db._
import org.broadinstitute.dsde.workbench.leonardo.http._
import org.broadinstitute.dsde.workbench.leonardo.monitor.DateAccessedUpdater._
import org.broadinstitute.dsde.workbench.model.google.GoogleProject
import org.broadinstitute.dsde.workbench.openTelemetry.OpenTelemetryMetrics
import slick.dbio.DBIO

import scala.concurrent.ExecutionContext
import scala.concurrent.duration.FiniteDuration

class DateAccessedUpdater[F[_]: ContextShift: Timer](
  config: DateAccessedUpdaterConfig,
  runtimeQueue: InspectableQueue[F, RuntimeUpdateDateAccessedMessage],
  appQueue: InspectableQueue[F, AppUpdateDateAccessedMessage]
)(implicit F: Concurrent[F], metrics: OpenTelemetryMetrics[F], dbRef: DbReference[F], ec: ExecutionContext) {

  val process: Stream[F, Unit] =
    (Stream.sleep[F](config.interval) ++ Stream.eval(checkRuntimes) ++ Stream.eval(checkApps)).repeat

  private[monitor] val checkRuntimes: F[Unit] =
    runtimeQueue.tryDequeueChunk1(config.maxUpdate).flatMap { chunks =>
      chunks
        .traverse(c =>
          runtimeMessagesToUpdate(c.toChain)
            .traverse(updateRuntimeDateAccessed)
        )
        .void
    }

  private[monitor] val checkApps: F[Unit] =
    appQueue.tryDequeueChunk1(config.maxUpdate).flatMap { chunks =>
      chunks
        .traverse(c =>
          appMessagesToUpdate(c.toChain)
            .traverse(updateAppDateAccessed)
        )
        .void
    }

  private def updateRuntimeDateAccessed(msg: RuntimeUpdateDateAccessedMessage): F[Unit] =
    metrics.incrementCounter("jupyterAccessCount") >>
      (clusterQuery.clearKernelFoundBusyDateByProjectAndName(msg.googleProject, msg.runtimeName, msg.dateAccessed) >>
        clusterQuery.updateDateAccessedByProjectAndName(msg.googleProject, msg.runtimeName, msg.dateAccessed)).transaction.void

  private def updateAppDateAccessed(msg: AppUpdateDateAccessedMessage): F[Unit] =
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
  implicit val runtimeUpdateDateAccessMessageOrder: Order[RuntimeUpdateDateAccessedMessage] =
    Order.fromLessThan[RuntimeUpdateDateAccessedMessage] { (msg1, msg2) =>
      if (msg1.googleProject == msg2.googleProject && msg1.runtimeName == msg2.runtimeName)
        msg1.dateAccessed.toEpochMilli < msg2.dateAccessed.toEpochMilli
      else
        false //we don't really care about order if they're not the same runtime, but we just need an Order if they're the same
    }

  implicit val appUpdateDateAccessMessageOrder: Order[AppUpdateDateAccessedMessage] =
    Order.fromLessThan[AppUpdateDateAccessedMessage] { (msg1, msg2) =>
      if (msg1.googleProject == msg2.googleProject && msg1.appName == msg2.appName)
        msg1.dateAccessed.toEpochMilli < msg2.dateAccessed.toEpochMilli
      else
        false //we don't really care about order if they're not the same app, but we just need an Order if they're the same
    }

  // group all messages by googleProject and runtimeName, and discard all older messages for the same runtime
  def runtimeMessagesToUpdate(
    messages: Chain[RuntimeUpdateDateAccessedMessage]
  ): List[RuntimeUpdateDateAccessedMessage] = {
    messages.groupBy(m => s"${m.runtimeName.asString}/${m.googleProject.value}").toList.traverse {
      case (_, messages) =>
        messages.toChain.toList.sorted.lastOption
    }
  }.getOrElse(List.empty)

  def appMessagesToUpdate(
    messages: Chain[AppUpdateDateAccessedMessage]
  ): List[AppUpdateDateAccessedMessage] = {
    messages.groupBy(m => s"${m.appName.value}/${m.googleProject.value}").toList.traverse {
      case (_, messages) =>
        messages.toChain.toList.sorted.lastOption
    }
  }.getOrElse(List.empty)
}

final case class DateAccessedUpdaterConfig(interval: FiniteDuration, maxUpdate: Int, queueSize: Int)
final case class RuntimeUpdateDateAccessedMessage(runtimeName: RuntimeName,
                                                  googleProject: GoogleProject,
                                                  dateAccessed: Instant)
final case class AppUpdateDateAccessedMessage(appName: AppName, googleProject: GoogleProject, dateAccessed: Instant)
