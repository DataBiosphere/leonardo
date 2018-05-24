package org.broadinstitute.dsde.workbench.leonardo.monitor

import java.time.Instant

import akka.actor.{Actor, Props}

import org.broadinstitute.dsde.workbench.leonardo.db.DbReference
import org.broadinstitute.dsde.workbench.leonardo.model.google.ClusterName
import org.broadinstitute.dsde.workbench.model.google.GoogleProject
import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.dsde.workbench.leonardo.config.AutoFreezeConfig
import org.broadinstitute.dsde.workbench.leonardo.monitor.ClusterDateAccessedActor.{Flush, UpdateDateAccessed}
import scala.concurrent.duration._

object ClusterDateAccessedActor {

  def props(autoFreezeConfig: AutoFreezeConfig, dbReference: DbReference): Props =
    Props(new ClusterDateAccessedActor(autoFreezeConfig, dbReference))

  sealed trait ClusterAccessDateMessage

  case class UpdateDateAccessed(clusterName: ClusterName, googleProject: GoogleProject, dateAccessed: Instant) extends ClusterAccessDateMessage

  case object Flush extends ClusterAccessDateMessage

}

class ClusterDateAccessedActor(autoFreezeConfig: AutoFreezeConfig, dbRef: DbReference) extends Actor with LazyLogging {

  var dateAccessedMap: Map[(ClusterName, GoogleProject), Instant] = Map.empty

  import context._

  override def preStart(): Unit = {
    super.preStart()
    system.scheduler.schedule(autoFreezeConfig.dateAccessedMonitorScheduler, autoFreezeConfig.dateAccessedMonitorScheduler, self, Flush)
  }

  override def receive: Receive = {
    case UpdateDateAccessed(clusterName, googleProject, dateAccessed) =>
      dateAccessedMap = dateAccessedMap + ((clusterName, googleProject) -> dateAccessed)

    case Flush => {
      dateAccessedMap.map(da => updateDateAccessed(da._1._1, da._1._2, da._2))
      dateAccessedMap = Map.empty
    }
  }

  private def updateDateAccessed(clusterName: ClusterName, googleProject: GoogleProject, dateAccessed: Instant) = {
    logger.info(s"Update dateAccessed for $clusterName in project $googleProject to $dateAccessed")
    dbRef.inTransaction { dataAccess =>
      dataAccess.clusterQuery.updateDateAccessedByProjectAndName(googleProject, clusterName, dateAccessed)
    }
  }
}
