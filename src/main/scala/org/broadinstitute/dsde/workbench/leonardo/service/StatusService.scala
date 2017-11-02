package org.broadinstitute.dsde.workbench.leonardo.service

import akka.actor.ActorSystem
import akka.pattern.ask
import akka.util.Timeout
import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.dsde.workbench.leonardo.config.DataprocConfig
import org.broadinstitute.dsde.workbench.leonardo.dao.{DataprocDAO, SamDAO}
import org.broadinstitute.dsde.workbench.leonardo.db.DbReference
import org.broadinstitute.dsde.workbench.util.health.HealthMonitor.GetCurrentStatus
import org.broadinstitute.dsde.workbench.util.health.{HealthMonitor, StatusCheckResponse, SubsystemStatus}
import org.broadinstitute.dsde.workbench.util.health.Subsystems._

import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration._

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
    )
  }

  private def checkGoogleDataproc(): Future[SubsystemStatus] = {
    logger.info("Checking Google Dataproc connection")
    // Does a 'list clusters' in Leo's project
    gdDAO.listClusters(dataprocConfig.leoGoogleProject).map(_ => HealthMonitor.OkStatus)
  }

  private def checkDatabase: Future[SubsystemStatus] = {
    logger.info("Checking database")
    dbRef.inTransaction(_.sqlDBStatus()).map(_ => HealthMonitor.OkStatus)
  }

  private def checkSam: Future[SubsystemStatus] = {
    logger.info("Checking Sam")
    samDAO.getStatus()
  }

}
