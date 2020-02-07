package org.broadinstitute.dsde.workbench.leonardo
package monitor

import java.time.{Duration, Instant}

import akka.actor.{Actor, Props, Timers}
import cats.effect.IO
import cats.implicits._
import com.google.api.client.googleapis.json.GoogleJsonResponseException
import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.dsde.workbench.google.GoogleProjectDAO
import org.broadinstitute.dsde.workbench.leonardo.config.ZombieClusterConfig
import org.broadinstitute.dsde.workbench.leonardo.dao.google.GoogleDataprocDAO
import org.broadinstitute.dsde.workbench.leonardo.db.{clusterErrorQuery, clusterQuery, DbReference}
import org.broadinstitute.dsde.workbench.leonardo.model.Cluster
import org.broadinstitute.dsde.workbench.leonardo.model.google.ClusterStatus
import org.broadinstitute.dsde.workbench.leonardo.monitor.ZombieClusterMonitor._
import org.broadinstitute.dsde.workbench.model.google.GoogleProject
import org.broadinstitute.dsde.workbench.newrelic.NewRelicMetrics

import scala.concurrent.Future

object ZombieClusterMonitor {

  def props(config: ZombieClusterConfig,
            gdDAO: GoogleDataprocDAO,
            googleProjectDAO: GoogleProjectDAO,
            dbRef: DbReference[IO])(implicit metrics: NewRelicMetrics[IO]): Props =
    Props(new ZombieClusterMonitor(config, gdDAO, googleProjectDAO, dbRef))

  sealed trait ZombieClusterMonitorMessage
  case object DetectZombieClusters extends ZombieClusterMonitorMessage
  case object TimerKey extends ZombieClusterMonitorMessage
}

/**
 * This monitor periodically sweeps the Leo database and checks for clusters which no longer exist in Google.
 */
class ZombieClusterMonitor(config: ZombieClusterConfig,
                           gdDAO: GoogleDataprocDAO,
                           googleProjectDAO: GoogleProjectDAO,
                           dbRef: DbReference[IO])(implicit metrics: NewRelicMetrics[IO])
    extends Actor
    with Timers
    with LazyLogging {
  import context._

  override def preStart(): Unit = {
    super.preStart()
    timers.startPeriodicTimer(TimerKey, DetectZombieClusters, config.zombieCheckPeriod)
  }

  override def receive: Receive = {
    case DetectZombieClusters =>
      val now = Instant.now()
      // Get active clusters from the Leo DB, grouped by project
      val zombieClusters = getActiveClustersFromDatabase.unsafeToFuture().flatMap { clusterMap =>
        clusterMap.toList.flatTraverse {
          case (project, clusters) =>
            // Check if the project is active
            isProjectActiveInGoogle(project).flatMap {
              case true =>
                // If the project is active, check each individual cluster
                logger.debug(s"Project ${project.value} containing ${clusters.size} clusters is active in Google")
                clusters.toList.traverseFilter { cluster =>
                  isClusterActiveInGoogle(cluster, now).map {
                    case true =>
                      logger.debug(s"Cluster ${cluster.projectNameString} is active in Google")
                      None
                    case false =>
                      logger.info(s"Cluster ${cluster.projectNameString} is a zombie!")
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
          handleZombieCluster(cluster).unsafeToFuture()
        }
      }

  }

  private def getActiveClustersFromDatabase: IO[Map[GoogleProject, Seq[Cluster]]] =
    dbRef.inTransaction {
      clusterQuery.listActiveWithLabels.map { clusters =>
        clusters.groupBy(_.googleProject)
      }
    }

  private def isProjectActiveInGoogle(googleProject: GoogleProject): Future[Boolean] =
    // Check the project and its billing info
    (googleProjectDAO.isProjectActive(googleProject.value), googleProjectDAO.isBillingActive(googleProject.value))
      .mapN(_ && _)
      .recover {
        //if we fail because of a permission error, we consider the cluster a zombie, as the permissions have been clean-up elsewhere
        //this occurs in the case of free credits projects, which are managed elsewhere
        case e: GoogleJsonResponseException if e.getStatusCode == 403 =>
          logger.info(
            s"Unable to check status of project ${googleProject.value} for zombie cluster detection due to a 403 from google. We are assuming this is a free credits project that has been cleaned up, and zombifying",
            e
          )
          false
        case e =>
          logger.warn(s"Unable to check status of project ${googleProject.value} for zombie cluster detection", e)
          true
      }

  private def isClusterActiveInGoogle(cluster: Cluster, now: Instant): Future[Boolean] = {
    val secondsSinceClusterCreation: Long = Duration.between(cluster.auditInfo.createdDate, now).getSeconds
    //this or'd with the google cluster status gives creating clusters a grace period before they are marked as zombies
    val isWithinHangTolerance = cluster.status == ClusterStatus.Creating && secondsSinceClusterCreation < config.creationHangTolerance.toSeconds

    gdDAO.getClusterStatus(cluster.googleProject, cluster.clusterName) map { clusterStatus =>
      (ClusterStatus.activeStatuses contains clusterStatus) || isWithinHangTolerance
    } recover {
      case e =>
        logger.warn(s"Unable to check status of cluster ${cluster.projectNameString} for zombie cluster detection", e)
        true
    }
  }

  private def handleZombieCluster(cluster: Cluster): IO[Unit] =
    for {
      _ <- IO(logger.info(s"Deleting zombie cluster: ${cluster.projectNameString}"))
      _ <- metrics.incrementCounter("zombieClusters")
      now <- IO(Instant.now)
      _ <- dbRef.inTransaction {
        for {
          _ <- clusterQuery.completeDeletion(cluster.id, now)
          error = ClusterError("An underlying resource was removed in Google. Cluster has been marked deleted in Leo.",
                               -1,
                               now)
          _ <- clusterErrorQuery.save(cluster.id, error)
        } yield ()
      }
    } yield ()
}
