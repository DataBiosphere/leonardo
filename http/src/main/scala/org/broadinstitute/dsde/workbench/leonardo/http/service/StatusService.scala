package org.broadinstitute.dsde.workbench.leonardo
package http
package service

import cats.effect.{ContextShift, IO, Timer}
import cats.mtl.Ask
import cats.syntax.all._
import fs2.Stream
import io.chrisdavenport.log4cats.Logger
import org.broadinstitute.dsde.workbench.leonardo.algebra.SamDAO
import org.broadinstitute.dsde.workbench.leonardo.db.DbReference
import org.broadinstitute.dsde.workbench.model.TraceId
import org.broadinstitute.dsde.workbench.util.health.HealthMonitor.UnknownStatus
import org.broadinstitute.dsde.workbench.util.health.Subsystems._
import org.broadinstitute.dsde.workbench.util.health.{HealthMonitor, StatusCheckResponse, SubsystemStatus}

import scala.collection.mutable
import java.util.UUID
import scala.concurrent.duration._

class StatusService(
  samDAO: SamDAO[IO],
  dbRef: DbReference[IO],
  initialDelay: FiniteDuration = Duration.Zero,
  pollInterval: FiniteDuration = 1 minute
)(implicit timer: Timer[IO], logger: Logger[IO], cs: ContextShift[IO]) {
  val defaultStaleThreshold = 15 minutes

  val subsystems = List(Sam, Database)

  val data: mutable.Map[Subsystem, (SubsystemStatus, Long)] = {
    val now = System.currentTimeMillis
    val builder = mutable.Map.newBuilder[Subsystem, (SubsystemStatus, Long)]
    builder.addAll(subsystems.map(s => (s -> (UnknownStatus -> now))))
    builder.result()
  }

  val process: Stream[IO, Unit] = Stream.sleep[IO](initialDelay) ++
    (Stream.sleep[IO](pollInterval) ++ Stream.eval(
      checkAll()
        .handleErrorWith(e => logger.error(e)("Unexpected error occurred during status checking monitoring"))
    )).repeat

  private def checkAll(): IO[Unit] =
    subsystems.traverse { system =>
      for {
        status <- system match {
          case Sam      => logFailures(Sam, checkSam)
          case Database => logFailures(Database, checkDatabase)
          case x =>
            throw new RuntimeException(
              s"this should never happen. Leonardo should only check Sam, and Database for status checks (${x} is invalid subsystem)"
            )
        }
        now <- nowInstant
        _ <- IO(data.addOne(system -> (status -> now.toEpochMilli)))
      } yield ()

    }.void

  def getStatus(): IO[StatusCheckResponse] =
    for {
      now <- nowInstant
    } yield {
      val processed = data.view.mapValues {
        case (_, t) if now.toEpochMilli - t > defaultStaleThreshold.toMillis => UnknownStatus
        case (status, _)                                                     => status
      }.toMap //toMap is needed for scala 2.13
      // overall status is ok iff all subsystems are ok
      val overall = processed.forall(_._2.ok)
      StatusCheckResponse(overall, processed)
    }

  // Logs warnings if a subsystem status check fails
  def logFailures(subsystem: Subsystem, subsysmteStatus: IO[SubsystemStatus]): IO[SubsystemStatus] =
    subsysmteStatus.attempt.flatMap {
      case Right(status) if !status.ok =>
        logger.warn(s"Subsystem [$subsystem] reported error status: $status") >> IO.pure(
          status
        )

      case Right(s) =>
        logger.debug(s"Subsystem [$subsystem] is OK") >> IO.pure(s)
      case Left(e) =>
        logger
          .warn(e)(s"Failure checking status for subsystem [$subsystem]: ${e.getMessage}") >> IO.raiseError(e)
    }

  private def checkDatabase: IO[SubsystemStatus] =
    logger.debug("Checking database connection") >>
      dbRef.dataAccess
        .sqlDBStatus()
        .transaction(dbRef)
        .as(HealthMonitor.OkStatus)

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
