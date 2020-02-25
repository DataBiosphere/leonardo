package org.broadinstitute.dsde.workbench.leonardo
package monitor

import java.time.{Duration, Instant}

import akka.actor.{Actor, Props, Timers}
import cats.data.Chain
import cats.effect.concurrent.Semaphore
import cats.effect.{ContextShift, IO}
import cats.implicits._
import com.google.api.client.googleapis.json.GoogleJsonResponseException
import io.chrisdavenport.log4cats.Logger
import org.broadinstitute.dsde.workbench.google.GoogleProjectDAO
import org.broadinstitute.dsde.workbench.leonardo.config.ZombieClusterConfig
import org.broadinstitute.dsde.workbench.leonardo.dao.google.GoogleDataprocDAO
import org.broadinstitute.dsde.workbench.leonardo.db._
import org.broadinstitute.dsde.workbench.leonardo.model.google.ClusterStatus
import org.broadinstitute.dsde.workbench.leonardo.monitor.ZombieClusterMonitor._
import org.broadinstitute.dsde.workbench.model.google.GoogleProject
import org.broadinstitute.dsde.workbench.newrelic.NewRelicMetrics

object ZombieClusterMonitor {

  def props(
    config: ZombieClusterConfig,
    gdDAO: GoogleDataprocDAO,
    googleProjectDAO: GoogleProjectDAO,
    dbRef: DbReference[IO]
  )(implicit metrics: NewRelicMetrics[IO], cs: ContextShift[IO], logger: Logger[IO]): Props =
    Props(new ZombieClusterMonitor(config, gdDAO, googleProjectDAO, dbRef))

  sealed trait ZombieClusterMonitorMessage
  case object DetectZombieClusters extends ZombieClusterMonitorMessage
  case object TimerKey extends ZombieClusterMonitorMessage
}

/**
 * This monitor periodically sweeps the Leo database and checks for clusters which no longer exist in Google.
 */
class ZombieClusterMonitor(
  config: ZombieClusterConfig,
  gdDAO: GoogleDataprocDAO,
  googleProjectDAO: GoogleProjectDAO,
  dbRef: DbReference[IO]
)(implicit metrics: NewRelicMetrics[IO], cs: ContextShift[IO], logger: Logger[IO])
    extends Actor
    with Timers {
  import context._

  val concurrency = 20

  override def preStart(): Unit = {
    super.preStart()
    timers.startTimerWithFixedDelay(TimerKey, DetectZombieClusters, config.zombieCheckPeriod)
  }

  override def receive: Receive = {
    case DetectZombieClusters =>
      (for {
        start <- IO(Instant.now)
        semaphore <- Semaphore[IO](concurrency)
        // Get active clusters from the Leo DB, grouped by project
        clusterMap <- getActiveClustersFromDatabase
        _ <- logger.info(
          s"Starting zombie detection across ${clusterMap.size} projects with concurrency of $concurrency"
        )
        zombies <- clusterMap.toList.parFlatTraverse {
          case (project, clusters) =>
            semaphore.withPermit(
              // Check if the project is active
              isProjectActiveInGoogle(project).flatMap {
                case true =>
                  // If the project is active, check each individual cluster
                  clusters.toList.traverseFilter { cluster =>
                    isClusterActiveInGoogle(cluster, start).map {
                      case true  => None
                      case false => Some(cluster)
                    }
                  }
                case false =>
                  // If the project is inactive, all clusters in the project are zombies
                  IO.pure(clusters.toList)
              }
            )
        }
        // Error out each detected zombie cluster
        _ <- zombies.parTraverse(handleZombieCluster)
        end <- IO(Instant.now)
        duration = Duration.between(start, end)
        _ <- logger.info(
          s"Detected ${zombies.size} zombie clusters in ${zombies.map(_.googleProject).toSet.size} projects. Elapsed = ${duration.getSeconds} seconds"
        )
      } yield ()).unsafeRunSync
  }

  private def getActiveClustersFromDatabase: IO[Map[GoogleProject, Chain[PotentialZombieCluster]]] =
    dbRef.inTransaction {
      ZombieMonitorQueries.listZombieQuery
    }

  private def isProjectActiveInGoogle(googleProject: GoogleProject): IO[Boolean] = {
    // Check the project and its billing info
    val res = for {
      isProjectActive <- IO.fromFuture(IO(googleProjectDAO.isProjectActive(googleProject.value)))
      isBillingActive <- IO.fromFuture(IO(googleProjectDAO.isBillingActive(googleProject.value)))
    } yield isProjectActive && isBillingActive

    res.recoverWith {
      case e: GoogleJsonResponseException if e.getStatusCode == 403 =>
        logger
          .info(e)(
            s"Unable to check status of project ${googleProject.value} for zombie cluster detection " +
              s"due to a 403 from google. We are assuming this is a free credits project that has been cleaned up, and zombifying"
          )
          .as(false)

      case e =>
        logger
          .warn(e)(s"Unable to check status of project ${googleProject.value} for zombie cluster detection")
          .as(true)
    }
  }

  private def isClusterActiveInGoogle(cluster: PotentialZombieCluster, now: Instant): IO[Boolean] = {
    val secondsSinceClusterCreation: Long = Duration.between(cluster.auditInfo.createdDate, now).getSeconds
    //this or'd with the google cluster status gives creating clusters a grace period before they are marked as zombies
    if (cluster.status == ClusterStatus.Creating && secondsSinceClusterCreation < config.creationHangTolerance.toSeconds) {
      IO.pure(true)
    } else {
      IO.fromFuture(IO(gdDAO.getClusterStatus(cluster.googleProject, cluster.clusterName)))
        .map(ClusterStatus.activeStatuses contains)
        .recoverWith {
          case e =>
            logger
              .warn(e)(
                s"Unable to check status of cluster ${cluster.googleProject} / ${cluster.clusterName} for zombie cluster detection"
              )
              .as(true)
        }
    }

  }

  private def handleZombieCluster(cluster: PotentialZombieCluster): IO[Unit] =
    for {
      _ <- logger.info(s"Deleting zombie cluster: ${cluster.googleProject} / ${cluster.clusterName}")
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
