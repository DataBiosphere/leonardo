package org.broadinstitute.dsde.workbench.leonardo
package service

import java.util.UUID

import akka.actor.ActorSystem
import akka.util.Timeout
import cats.effect.{ContextShift, IO}
import cats.implicits._
import cats.mtl.ApplicativeAsk
import akka.pattern.ask
import io.chrisdavenport.log4cats.Logger
import org.broadinstitute.dsde.workbench.leonardo.config.DataprocConfig
import org.broadinstitute.dsde.workbench.leonardo.dao.SamDAO
import org.broadinstitute.dsde.workbench.leonardo.dao.google.GoogleDataprocDAO
import org.broadinstitute.dsde.workbench.leonardo.db.DbReference
import org.broadinstitute.dsde.workbench.model.TraceId
import org.broadinstitute.dsde.workbench.util.health.HealthMonitor.GetCurrentStatus
import org.broadinstitute.dsde.workbench.util.health.Subsystems._
import org.broadinstitute.dsde.workbench.util.health.{HealthMonitor, StatusCheckResponse, SubsystemStatus}

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}

/**
 * Created by rtitle on 10/26/17.
 */
class StatusService(
  gdDAO: GoogleDataprocDAO,
  samDAO: SamDAO[IO],
  dbRef: DbReference,
  dataprocConfig: DataprocConfig,
  initialDelay: FiniteDuration = Duration.Zero,
  pollInterval: FiniteDuration = 1 minute
)(implicit system: ActorSystem, executionContext: ExecutionContext, logger: Logger[IO], cs: ContextShift[IO]) {
  implicit val askTimeout = Timeout(5.seconds)

  private val healthMonitor =
    system.actorOf(HealthMonitor.props(Set(GoogleDataproc, Sam, Database))(() => checkStatus()))
  system.scheduler.schedule(initialDelay, pollInterval, healthMonitor, HealthMonitor.CheckAll)

  def getStatus(): Future[StatusCheckResponse] =
    (healthMonitor ? GetCurrentStatus).asInstanceOf[Future[StatusCheckResponse]]

//  {
//    val res = for {
//      subSystems <- checkStatus.values.toList.parTraverse {
//        action =>
//          action.handleErrorWith(t => IO.pure(SubsystemStatus(false, Some(List(t.getMessage)))))
//      }
//      overall = subSystems.forall(_.ok)
//      systems = checkStatus.keys.zip(subSystems).toMap
//    } yield StatusCheckResponse(overall, systems)
//    res.unsafeToFuture()
//  }

  private def checkStatus(): Map[Subsystem, Future[SubsystemStatus]] =
    Map(
      GoogleDataproc -> checkGoogleDataproc,
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

  private def checkGoogleDataproc(): Future[SubsystemStatus] = {
    // Does a 'list clusters' in Leo's project.
    // Doesn't look at results, just checks if the request was successful.
    logger.debug("Checking Google Dataproc connection").unsafeToFuture()
    gdDAO.listClusters(dataprocConfig.leoGoogleProject).map(_ => HealthMonitor.OkStatus)
  }

  private def checkDatabase: Future[SubsystemStatus] = {
    logger.debug("Checking database connection").unsafeToFuture()
    dbRef.inTransaction(_.sqlDBStatus()).map(_ => HealthMonitor.OkStatus)
  }

  private def checkSam: IO[SubsystemStatus] = {
    implicit val traceId = ApplicativeAsk.const[IO, TraceId](TraceId(UUID.randomUUID()))

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
      .handleErrorWith { t =>
        logger.error(s"SAM is not healthy. ${t.getMessage}") >> IO.raiseError(t)
      }
  }

}
