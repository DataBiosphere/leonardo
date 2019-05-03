package org.broadinstitute.dsde.workbench.leonardo.monitor

import java.time.Instant
import java.time.Duration

import akka.actor.{Actor, Props, Timers}
import cats.implicits._
import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.dsde.workbench.google.GoogleProjectDAO
import org.broadinstitute.dsde.workbench.leonardo.config.ZombieClusterConfig
import org.broadinstitute.dsde.workbench.leonardo.dao.google.GoogleDataprocDAO
import org.broadinstitute.dsde.workbench.leonardo.db.DbReference
import org.broadinstitute.dsde.workbench.leonardo.model.google.ClusterStatus
import org.broadinstitute.dsde.workbench.leonardo.model.{Cluster, ClusterError}
import org.broadinstitute.dsde.workbench.leonardo.monitor.ZombieClusterMonitor._
import org.broadinstitute.dsde.workbench.model.google.GoogleProject

import scala.concurrent.Future

object ZombieClusterMonitor {

  def props(config: ZombieClusterConfig, gdDAO: GoogleDataprocDAO, googleProjectDAO: GoogleProjectDAO, dbRef: DbReference): Props = {
    Props(new ZombieClusterMonitor(config, gdDAO, googleProjectDAO, dbRef))
  }

  sealed trait ZombieClusterMonitorMessage
  case object DetectZombieClusters extends ZombieClusterMonitorMessage
  case object TimerKey extends ZombieClusterMonitorMessage
}

/**
  * This monitor periodically sweeps the Leo database and checks for clusters which no longer exist in Google.
  */
class ZombieClusterMonitor(config: ZombieClusterConfig, gdDAO: GoogleDataprocDAO, googleProjectDAO: GoogleProjectDAO, dbRef: DbReference) extends Actor with Timers with LazyLogging {
  import context._

  override def preStart(): Unit = {
    super.preStart()
    timers.startPeriodicTimer(TimerKey, DetectZombieClusters, config.zombieCheckPeriod)
  }

  override def receive: Receive = {
    case DetectZombieClusters =>
      // Get active clusters from the Leo DB, grouped by project
      val zombieClusters = getActiveClustersFromDatabase.flatMap { clusterMap =>
        clusterMap.toList.flatTraverse { case (project, clusters) =>
          // Check if the project is active
          isProjectActiveInGoogle(project).flatMap {
            case true =>
              // If the project is active, check each individual cluster
              logger.debug(s"Project ${project.value} containing ${clusters.size} clusters is active in Google")
              clusters.toList.traverseFilter { cluster =>
                isClusterActiveInGoogle(cluster).map {
                  case true =>
                    logger.debug(s"Cluster ${cluster.projectNameString} is active in Google")
                    None
                  case false =>
                    logger.debug(s"Cluster ${cluster.projectNameString} is a zombie!")
                    Some(cluster)
                }
              }
            case false =>
              // If the project is inactive, all clusters in the project are zombies
              logger.debug(s"Project ${project.value} containing ${clusters.size} clusters is inactive in Google")
              Future.successful(clusters.toList)
          }
        }
      }

      // Error out each detected zombie cluster
      zombieClusters.flatMap { cs =>
        logger.info(s"Detected ${cs.size} zombie clusters across ${cs.map(_.googleProject).toSet.size} projects.")
        cs.traverse { cluster =>
          handleZombieCluster(cluster)
        }
      }

  }

  private def getActiveClustersFromDatabase: Future[Map[GoogleProject, Seq[Cluster]]] = {
    dbRef.inTransaction {
      _.clusterQuery.listActive
    } map { clusters =>
      clusters.groupBy(_.googleProject)
    }
  }

  private def isProjectActiveInGoogle(googleProject: GoogleProject): Future[Boolean] = {
    // Check the project and its billing info
    (googleProjectDAO.isProjectActive(googleProject.value), googleProjectDAO.isBillingActive(googleProject.value))
      .mapN(_ && _)
      .recover { case e =>
        logger.warn(s"Unable to check status of project ${googleProject.value} for zombie cluster detection", e)
        true
      }
  }

  private def isClusterActiveInGoogle(cluster: Cluster): Future[Boolean] = {
    // Clusters in Creating status may not yet exist in Google. Clusters in creating (that are not hanging, as defined by config.creationHangTolerance) are considered active
    if (cluster.status == ClusterStatus.Creating) {

        val secondsSinceClusterCreation: Long = Duration.between(cluster.auditInfo.createdDate, Instant.now()).getSeconds

        //determine if the creating cluster has been hanging past the tolerance threshold
        if (secondsSinceClusterCreation > config.creationHangTolerance.toSeconds) {
           Future.successful(false)
        } else {
          Future.successful(true)
        }

    } else {
      // Check if status returned by GoogleDataprocDAO is an "active" status.
      gdDAO.getClusterStatus(cluster.googleProject, cluster.clusterName) map { clusterStatus =>
        ClusterStatus.activeStatuses contains clusterStatus
      } recover { case e =>
        logger.warn(s"Unable to check status of cluster ${cluster.projectNameString} for zombie cluster detection", e)
        true
      }
    }
  }

  private def handleZombieCluster(cluster: Cluster): Future[Unit] = {
    logger.info(s"Erroring zombie cluster: ${cluster.projectNameString}")
    dbRef.inTransaction { dataAccess =>
      for {
        _ <- dataAccess.clusterQuery.updateClusterStatus(cluster.id, ClusterStatus.Error)
        error = ClusterError("An underlying resource was removed in Google. Please delete and recreate your cluster.", -1, Instant.now)
        _ <- dataAccess.clusterErrorQuery.save(cluster.id, error)
      } yield ()
    }.void
  }
}
