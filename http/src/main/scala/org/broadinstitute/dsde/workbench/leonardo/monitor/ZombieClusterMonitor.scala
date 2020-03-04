package org.broadinstitute.dsde.workbench.leonardo
package monitor

import java.time.{Duration, Instant}

import akka.actor.{Actor, Props, Timers}
import cats.effect.concurrent.Semaphore
import cats.effect.{ContextShift, IO}
import cats.implicits._
import com.google.api.client.googleapis.json.GoogleJsonResponseException
import io.chrisdavenport.log4cats.Logger
import org.broadinstitute.dsde.workbench.google.GoogleProjectDAO
import org.broadinstitute.dsde.workbench.leonardo.config.ZombieClusterConfig
import org.broadinstitute.dsde.workbench.leonardo.dao.google.GoogleDataprocDAO
import org.broadinstitute.dsde.workbench.leonardo.db._
import org.broadinstitute.dsde.workbench.leonardo.http._
import org.broadinstitute.dsde.workbench.leonardo.monitor.ZombieClusterMonitor._
import org.broadinstitute.dsde.workbench.model.google.GoogleProject
import org.broadinstitute.dsde.workbench.newrelic.NewRelicMetrics

object ZombieClusterMonitor {

  def props(
    config: ZombieClusterConfig,
    gdDAO: GoogleDataprocDAO,
    googleProjectDAO: GoogleProjectDAO
  )(implicit metrics: NewRelicMetrics[IO], cs: ContextShift[IO], logger: Logger[IO], dbRef: DbReference[IO]): Props =
    Props(new ZombieClusterMonitor(config, gdDAO, googleProjectDAO))

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
  googleProjectDAO: GoogleProjectDAO
)(implicit metrics: NewRelicMetrics[IO], cs: ContextShift[IO], logger: Logger[IO], dbRef: DbReference[IO])
    extends Actor
    with Timers {
  import context._

  override def preStart(): Unit = {
    super.preStart()
    timers.startTimerWithFixedDelay(TimerKey, DetectZombieClusters, config.zombieCheckPeriod)
  }

  override def receive: Receive = {
    case DetectZombieClusters =>
      (for {
        start <- IO(Instant.now)
        semaphore <- Semaphore[IO](config.concurrency)
        // Get active clusters from the Leo DB, grouped by project
        clusterMap <- ZombieMonitorQueries.listZombieQuery.transaction
        _ <- logger.info(
          s"Starting zombie detection across ${clusterMap.size} projects with concurrency of ${config.concurrency}"
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
        _ <- zombies.parTraverse(zombie => semaphore.withPermit(handleZombieCluster(zombie)))
        end <- IO(Instant.now)
        duration = Duration.between(start, end)
        _ <- logger.info(
          s"Detected ${zombies.size} zombie clusters in ${zombies.map(_.googleProject).toSet.size} projects. Elapsed time = ${duration.getSeconds} seconds"
        )
      } yield ()).unsafeRunSync
  }

  private def isProjectActiveInGoogle(googleProject: GoogleProject): IO[Boolean] = {
    // Check the project and its billing info
    val res = for {
      isBillingActive <- IO.fromFuture(IO(googleProjectDAO.isBillingActive(googleProject.value)))
      // short circuit
      isProjectActive <- if (!isBillingActive) IO.pure(false)
      else IO.fromFuture(IO(googleProjectDAO.isProjectActive(googleProject.value)))
    } yield isProjectActive

    res.recoverWith {
      case e: GoogleJsonResponseException if e.getStatusCode == 403 =>
        logger
          .info(e)(
            s"Unable to check status of project ${googleProject.value} for zombie cluster detection " +
              s"due to a 403 from google. We are assuming this is a free credits project that has been cleaned up. " +
              s"Marking project as a zombie."
          )
          .as(false)

      case e =>
        logger
          .warn(e)(s"Unable to check status of project ${googleProject.value} for zombie cluster detection")
          .as(true)
    }
  }

  // TODO this is dataproc specific
  private def isClusterActiveInGoogle(runtime: PotentialZombieRuntime, now: Instant): IO[Boolean] = {
    val secondsSinceClusterCreation: Long = Duration.between(runtime.auditInfo.createdDate, now).getSeconds
    // this or'd with the google cluster status gives creating clusters a grace period before they are marked as zombies
    if (runtime.status == RuntimeStatus.Creating && secondsSinceClusterCreation < config.creationHangTolerance.toSeconds) {
      IO.pure(true)
    } else {
      IO.fromFuture(IO(gdDAO.getClusterStatus(runtime.googleProject, runtime.runtimeName)))
        .map(RuntimeStatus.activeStatuses contains)
        .recoverWith {
          case e =>
            logger
              .warn(e)(
                s"Unable to check status of cluster ${runtime.googleProject} / ${runtime.runtimeName} for zombie cluster detection"
              )
              .as(true)
        }
    }

  }

  private def handleZombieCluster(runtime: PotentialZombieRuntime): IO[Unit] =
    for {
      _ <- logger.info(s"Deleting zombie cluster: ${runtime.googleProject} / ${runtime.runtimeName}")
      _ <- metrics.incrementCounter("zombieClusters")
      now <- IO(Instant.now)
      _ <- dbRef.inTransaction {
        for {
          _ <- clusterQuery.completeDeletion(runtime.id, now)
          error = RuntimeError("An underlying resource was removed in Google. Cluster has been marked deleted in Leo.",
                               -1,
                               now)
          _ <- clusterErrorQuery.save(runtime.id, error)
        } yield ()
      }
    } yield ()
}
