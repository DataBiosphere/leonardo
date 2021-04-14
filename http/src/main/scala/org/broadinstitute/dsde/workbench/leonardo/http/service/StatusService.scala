package org.broadinstitute.dsde.workbench.leonardo
package http
package service

import java.util.UUID

import akka.actor.ActorSystem
import akka.pattern.ask
import akka.util.Timeout
import cats.effect.{ContextShift, IO}
import cats.mtl.Ask
import cats.syntax.all._
import org.typelevel.log4cats.Logger
import org.broadinstitute.dsde.workbench.leonardo.config.ApplicationConfig
import org.broadinstitute.dsde.workbench.leonardo.dao.SamDAO
import org.broadinstitute.dsde.workbench.leonardo.db.DbReference
import org.broadinstitute.dsde.workbench.model.TraceId
import org.broadinstitute.dsde.workbench.util.health.HealthMonitor.GetCurrentStatus
import org.broadinstitute.dsde.workbench.util.health.Subsystems._
import org.broadinstitute.dsde.workbench.util.health.{HealthMonitor, StatusCheckResponse, SubsystemStatus}

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}

class StatusService(
  samDAO: SamDAO[IO],
  dbRef: DbReference[IO],
  applicationConfig: ApplicationConfig,
  initialDelay: FiniteDuration = Duration.Zero,
  pollInterval: FiniteDuration = 1 minute
)(implicit system: ActorSystem, executionContext: ExecutionContext, logger: Logger[IO], cs: ContextShift[IO]) {
  implicit val askTimeout = Timeout(5.seconds)
  import dbRef._

  private val healthMonitor =
    system.actorOf(HealthMonitor.props(Set(Sam, Database))(() => checkStatus()))
  system.scheduler.scheduleWithFixedDelay(initialDelay, pollInterval, healthMonitor, HealthMonitor.CheckAll)

  def getStatus(): Future[StatusCheckResponse] =
    (healthMonitor ? GetCurrentStatus).asInstanceOf[Future[StatusCheckResponse]]

  private def checkStatus(): Map[Subsystem, Future[SubsystemStatus]] =
    Map(
      Sam -> checkSam.unsafeToFuture(),
      Database -> checkDatabase
    ).map(logFailures.tupled)

  // Logs warnings if a subsystem status check fails
  def logFailures: (Subsystem, Future[SubsystemStatus]) => (Subsystem, Future[SubsystemStatus]) =
    (subsystem, statusFuture) =>
      subsystem -> statusFuture.attempt.flatMap {
        case Right(status) if !status.ok =>
          logger.warn(s"Subsystem [$subsystem] reported error status: $status").unsafeToFuture() >> Future.successful(
            status
          )

        case Right(s) =>
          logger.debug(s"Subsystem [$subsystem] is OK").unsafeToFuture() >> Future.successful(s)
        case Left(e) =>
          logger.warn(s"Failure checking status for subsystem [$subsystem]: ${e.getMessage}").unsafeToFuture() >> Future
            .failed(e)
      }

  private def checkDatabase: Future[SubsystemStatus] = {
    logger.debug("Checking database connection").unsafeToFuture()
    inTransaction(dataAccess.sqlDBStatus()).map(_ => HealthMonitor.OkStatus).unsafeToFuture()
  }

  private def checkSam: IO[SubsystemStatus] = {
    implicit val traceId = Ask.const[IO, TraceId](TraceId(UUID.randomUUID()))

    logger.debug(s"Checking Sam status") >> samDAO.getStatus
      .map { statusCheckResponse =>
        // SamDAO returns a StatusCheckResponse. Convert to a SubsystemStatus for use by the health checker.
        val messages = statusCheckResponse.systems.toList match {
          case Nil => None
          case systems =>
            systems.flatTraverse[Option, String] {
              case (subsystem, subSystemStatus) =>
                subSystemStatus.messages.map(msgs => msgs.map(m => s"${subsystem.value} -> $m"))
            }
        }
        SubsystemStatus(statusCheckResponse.ok, messages)
      }
      .handleErrorWith(t => logger.error(s"SAM is not healthy. ${t.getMessage}") >> IO.raiseError(t))
  }

}
