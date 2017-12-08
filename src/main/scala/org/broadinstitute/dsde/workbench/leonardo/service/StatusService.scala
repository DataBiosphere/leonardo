package org.broadinstitute.dsde.workbench.leonardo.service

import akka.actor.ActorSystem
import akka.pattern.ask
import akka.util.Timeout

import cats.implicits._
import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.dsde.workbench.leonardo.config.DataprocConfig
import org.broadinstitute.dsde.workbench.leonardo.dao.{DataprocDAO, SamDAO}
import org.broadinstitute.dsde.workbench.leonardo.db.DbReference
import org.broadinstitute.dsde.workbench.util.health.HealthMonitor.GetCurrentStatus
import org.broadinstitute.dsde.workbench.util.health.{HealthMonitor, StatusCheckResponse, SubsystemStatus}
import org.broadinstitute.dsde.workbench.util.health.Subsystems._

import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration._
import scala.util.{Failure, Success}

/**
  * Created by rtitle on 10/26/17.
  */
class StatusService(gdDAO: DataprocDAO,
                    samDAO: SamDAO,
                    dbRef: DbReference,
                    dataprocConfig: DataprocConfig,
                    initialDelay: FiniteDuration = Duration.Zero,
                    pollInterval: FiniteDuration = 1 minute)
                   (implicit system: ActorSystem, executionContext: ExecutionContext) extends LazyLogging {
  implicit val askTimeout = Timeout(5.seconds)

  private val healthMonitor = system.actorOf(HealthMonitor.props(Set(GoogleDataproc, Sam, Database))(checkStatus _))
  system.scheduler.schedule(initialDelay, pollInterval, healthMonitor, HealthMonitor.CheckAll)

  def getStatus(): Future[StatusCheckResponse] = (healthMonitor ? GetCurrentStatus).asInstanceOf[Future[StatusCheckResponse]]

  private def checkStatus(): Map[Subsystem, Future[SubsystemStatus]] = {
    Map(
      GoogleDataproc -> checkGoogleDataproc,
      Sam -> checkSam,
      Database -> checkDatabase
    ).map(logFailures.tupled)
  }

  // Logs warnings if a subsystem status check fails
  def logFailures: (Subsystem, Future[SubsystemStatus]) => (Subsystem, Future[SubsystemStatus]) = (subsystem, statusFuture) =>
    subsystem -> statusFuture.andThen {
      case Success(status) if !status.ok =>
        logger.warn(s"Subsystem [$subsystem] reported error status: $status")
      case Success(_) =>
        logger.debug(s"Subsystem [$subsystem] is OK")
      case Failure(e) =>
        logger.warn(s"Failure checking status for subsystem [$subsystem]: ${e.getMessage}")
    }
  
  private def checkGoogleDataproc(): Future[SubsystemStatus] = {
    // Does a 'list clusters' in Leo's project.
    // Doesn't look at results, just checks if the request was successful.
    logger.debug("Checking Google Dataproc connection")
    gdDAO.listClusters(dataprocConfig.leoGoogleProject).map(_ => HealthMonitor.OkStatus)
  }

  private def checkDatabase: Future[SubsystemStatus] = {
    logger.debug("Checking database connection")
    dbRef.inTransaction(_.sqlDBStatus()).map(_ => HealthMonitor.OkStatus)
  }

  private def checkSam: Future[SubsystemStatus] = {
    logger.debug("Checking Sam status")
    samDAO.getStatus().map { statusCheckResponse =>
      // SamDAO returns a StatusCheckResponse. Convert to a SubsystemStatus for use by the health checker.
      val messages = statusCheckResponse.systems.toList.traverse[Option, String] { case (subsystem, subSystemStatus) =>
        subSystemStatus.messages.map(msgs => s"${subsystem.value} -> ${msgs.mkString(", ")}")
      } match {
        // It's weird that I need to do this. Traversing an empty list always returns Some(List()) instead of None.
        case Some(Nil) => None
        case x => x
      }
      SubsystemStatus(statusCheckResponse.ok, messages)
    }
  }

}
