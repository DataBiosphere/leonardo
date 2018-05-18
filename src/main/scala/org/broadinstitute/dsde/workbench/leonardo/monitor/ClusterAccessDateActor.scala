package org.broadinstitute.dsde.workbench.leonardo.monitor

import java.time.Instant

import akka.actor.Actor
import org.broadinstitute.dsde.workbench.leonardo.db.DbReference
import org.broadinstitute.dsde.workbench.leonardo.model.Cluster
import org.broadinstitute.dsde.workbench.leonardo.model.google.ClusterName
import org.broadinstitute.dsde.workbench.model.google.GoogleProject
import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.dsde.workbench.leonardo.config.{AutoFreezeConfig, ProxyConfig}


sealed trait ClusterAccessDateMessage
case class updateDateAccessed(clusterName: ClusterName, googleProject: GoogleProject, dateAccessed: Instant) extends ClusterAccessDateMessage
case class flush() extends ClusterAccessDateMessage


class ClusterDateAccessedActor(autoFreezeConfig: AutoFreezeConfig, dbRef: DbReference) extends Actor with LazyLogging {

  var dateAccessedMap: Map[(ClusterName, GoogleProject), Instant] = Map.empty

  override def receive: Receive = {
    case updateDateAccessed(clusterName, googleProject, dateAccessed) =>
      dateAccessedMap = dateAccessedMap + ((clusterName, googleProject) -> dateAccessed)
    case flush() =>
      dateAccessedMap map {
        case (nameProject, time) => {
          dbRef.inTransaction{ dataAccess =>
            dataAccess.clusterQuery.getActiveClusterByName(nameProject._2, nameProject._1) map {
              case (c:Some[Cluster]) => c.map(cl => {
                val a = dataAccess.clusterQuery.updateDateAccessed(cl.googleId, time)
                logger.info(s"Updating Date accessed for cluster ${cl.clusterName} in project ${cl.googleProject} to $time")
                a})
            }
          }
        }
      }
  }
}
