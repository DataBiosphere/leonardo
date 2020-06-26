package org.broadinstitute.dsde.workbench.leonardo
package monitor

import java.time.Instant
import java.util.concurrent.TimeUnit

import cats.Parallel
import cats.effect.concurrent.Semaphore
import cats.effect.{Concurrent, ContextShift, IO, Timer}
import cats.implicits._
import cats.mtl.ApplicativeAsk
import com.google.api.client.googleapis.json.GoogleJsonResponseException
import fs2.Stream
import io.chrisdavenport.log4cats.Logger
import org.broadinstitute.dsde.workbench.google.GoogleProjectDAO
import org.broadinstitute.dsde.workbench.leonardo.config.ZombieRuntimeMonitorConfig
import org.broadinstitute.dsde.workbench.leonardo.db._
import org.broadinstitute.dsde.workbench.leonardo.http._
import org.broadinstitute.dsde.workbench.leonardo.util.{DeleteRuntimeParams, GetRuntimeStatusParams, RuntimeInstances}
import org.broadinstitute.dsde.workbench.model.TraceId
import org.broadinstitute.dsde.workbench.model.google.GoogleProject
import org.broadinstitute.dsde.workbench.openTelemetry.OpenTelemetryMetrics

import scala.concurrent.ExecutionContext

/**
 * This monitor periodically sweeps the Leo database and checks for and handles zombie runtimes.
 * There are two types of zombie runtimes:
 * Active zombie : a runtime that is deleted on the Google side, but still marked as active in the Leo DB
 * Inactive zombie : a runtime that is in Deleted status in the Leo DB, but still running in Google
 */
class ZombieRuntimeMonitor[F[_]: Parallel: ContextShift: Timer](
  config: ZombieRuntimeMonitorConfig,
  googleProjectDAO: GoogleProjectDAO
)(implicit F: Concurrent[F],
  metrics: OpenTelemetryMetrics[F],
  logger: Logger[F],
  dbRef: DbReference[F],
  ec: ExecutionContext,
  cs: ContextShift[IO],
  runtimes: RuntimeInstances[F]) {

  val process: Stream[F, Unit] =
    (Stream.sleep[F](config.zombieCheckPeriod) ++ Stream.eval(
      zombieCheck
        .handleErrorWith(e => logger.error(e)("Unexpected error occurred during zombie monitoring"))
    )).repeat

  private[monitor] val zombieCheck: F[Unit] =
    for {
      start <- Timer[F].clock.realTime(TimeUnit.MILLISECONDS)
      implicit0(traceId: ApplicativeAsk[F, TraceId]) = ApplicativeAsk.const[F, TraceId](
        TraceId(s"fromZombieCheck_${start}")
      )
      startInstant = Instant.ofEpochMilli(start)
      semaphore <- Semaphore[F](config.concurrency)

      // Get active runtimes from the Leo DB, grouped by project
      activeRuntimeMap <- ZombieMonitorQueries.listActiveZombieQuery.transaction

      _ <- logger.info(
        s"Starting active zombie detection across ${activeRuntimeMap.size} projects with concurrency of ${config.concurrency}"
      )
      activeZombies <- activeRuntimeMap.toList.parFlatTraverse[F, ZombieCandidate] {
        case (project, zombieCandidates) =>
          semaphore.withPermit(
            // Check if the project is active
            isProjectActiveInGoogle(project).flatMap {
              case true =>
                // If the project is active, check each individual runtime
                zombieCandidates.toList.traverseFilter {
                  candidate =>
                    // If the runtime is less than one minute old, it may not exist in Google because it is still Provisioning, so it's not a zombie
                    isRuntimeActiveInGoogle(candidate.googleProject,
                                            candidate.runtimeName,
                                            candidate.cloudService,
                                            startInstant).attempt
                      .flatMap {
                        case Left(e) =>
                          logger
                            .warn(e)(
                              s"Unable to check status of runtime ${candidate.googleProject.value} / ${candidate.runtimeName.asString} for active zombie runtime detection"
                            )
                            .as(None)
                        case Right(true) =>
                          F.pure(None)
                        case Right(false) =>
                          // it's an active zombie if it's _inactive_ in Google
                          F.pure(zombieIfOlderThanCreationHangTolerance(candidate, startInstant))
                      }
                }
              case false =>
                // If the project is inactive, all runtimes in the project are zombies
                F.pure(zombieCandidates.toList)
            }
          )
      }

      // Get all deleted runtimes that haven't been confirmed from the Leo DB
      unconfirmedDeletedRuntimes <- ZombieMonitorQueries.listInactiveZombieQuery.transaction

      _ <- logger.info(
        s"Starting inactive zombie detection within ${unconfirmedDeletedRuntimes.size} runtimes with concurrency of ${config.concurrency}"
      )

      inactiveZombies <- unconfirmedDeletedRuntimes
        .parTraverse[F, Option[ZombieCandidate]] { candidate =>
          semaphore.withPermit {
            isRuntimeActiveInGoogle(candidate.googleProject,
                                    candidate.runtimeName,
                                    candidate.cloudService,
                                    startInstant).attempt.flatMap {
              case Left(e) =>
                logger
                  .warn(e)(
                    s"Unable to check status of runtime ${candidate.googleProject.value} / ${candidate.runtimeName.asString} for inactive zombie runtime detection"
                  )
                  .as(None)
              case Right(true) =>
                // it's an inactive zombie if it's _active_ in Google
                F.pure(Some(candidate))
              case Right(false) =>
                updateRuntimeAsConfirmedDeleted(candidate.id).as(None)
            }
          }
        }
        .map(_.flattenOption)

      // Delete runtime on Leo side if active zombie, or on Google side if inactive zombie
      _ <- activeZombies.parTraverse(zombie => semaphore.withPermit(handleActiveZombieRuntime(zombie, startInstant)))
      _ <- inactiveZombies.parTraverse(zombie =>
        semaphore.withPermit(handleInactiveZombieRuntime(zombie, startInstant))
      )
      end <- Timer[F].clock.realTime(TimeUnit.MILLISECONDS)
      duration = end - start
      _ <- logger.info(
        s"Detected ${activeZombies.size} active zombie runtimes in ${activeZombies.map(_.googleProject).toSet.size} projects " +
          s"and ${inactiveZombies.size} inactive zombie runtimes. " +
          s"Elapsed time = ${duration} milli seconds."
      )
    } yield ()

  // Puts a label on a deleted runtime to show that we've confirmed the google runtime's deletion
  private def updateRuntimeAsConfirmedDeleted(runtimeId: Long): F[Unit] =
    dbRef.inTransaction {
      labelQuery.save(runtimeId, LabelResourceType.Runtime, config.deletionConfirmationLabelKey, "true")
    }.void

  private def isProjectActiveInGoogle(googleProject: GoogleProject): F[Boolean] = {
    // Check the project and its billing info
    val res = for {
      isBillingActive <- F.liftIO(IO.fromFuture(IO(googleProjectDAO.isBillingActive(googleProject.value))))
      // short circuit
      isProjectActive <- if (!isBillingActive) F.pure(false)
      else F.liftIO(IO.fromFuture(IO(googleProjectDAO.isProjectActive(googleProject.value))))
    } yield isProjectActive

    res.recoverWith {
      case e: GoogleJsonResponseException if e.getStatusCode == 403 =>
        logger
          .info(e)(
            s"Unable to check status of project ${googleProject.value} for zombie runtime detection " +
              s"due to a 403 from google. We are assuming this is a free credits project that has been cleaned up. " +
              s"Marking project as a zombie."
          )
          .as(false)

      case e =>
        logger
          .warn(e)(s"Unable to check status of project ${googleProject.value} for zombie runtime detection")
          .as(true)
    }
  }

  private def isRuntimeActiveInGoogle(googleProject: GoogleProject,
                                      runtimeName: RuntimeName,
                                      cloudService: CloudService,
                                      now: Instant)(implicit traceId: ApplicativeAsk[F, TraceId]): F[Boolean] =
    cloudService.interpreter
      .getRuntimeStatus(GetRuntimeStatusParams(googleProject, runtimeName, Some(config.gceZoneName)))
      .map(_.isActive)

  private def zombieIfOlderThanCreationHangTolerance(candidate: ZombieCandidate,
                                                     now: Instant): Option[ZombieCandidate] = {
    val milliSecondsSinceRuntimeCreation: Long = now.toEpochMilli - candidate.createdDate.toEpochMilli
    if (milliSecondsSinceRuntimeCreation < config.creationHangTolerance.toMillis) {
      None
    } else {
      Some(candidate)
    }
  }

  private def handleActiveZombieRuntime(zombie: ZombieCandidate,
                                        now: Instant)(implicit ev: ApplicativeAsk[F, TraceId]): F[Unit] =
    for {
      traceId <- ev.ask
      _ <- logger.info(
        s"${traceId.asString} | Deleting active zombie runtime: ${zombie.googleProject} / ${zombie.runtimeName}"
      ) //TODO: do we need to delete sam resource as well?
      _ <- metrics.incrementCounter("numOfActiveZombieRuntimes")
      _ <- dbRef.inTransaction {
        for {
          _ <- clusterQuery.completeDeletion(zombie.id, now)
          _ <- labelQuery
            .save(zombie.id, LabelResourceType.Runtime, config.deletionConfirmationLabelKey, "false")
          error = RuntimeError(
            s"An underlying resource was removed in Google. Runtime(${zombie.runtimeName.asString}) has been marked deleted in Leo.",
            -1,
            now
          )
          _ <- clusterErrorQuery.save(zombie.id, error)
        } yield ()
      }
    } yield ()

  private def handleInactiveZombieRuntime(zombie: ZombieCandidate,
                                          now: Instant)(implicit ev: ApplicativeAsk[F, TraceId]): F[Unit] =
    for {
      traceId <- ev.ask
      _ <- logger.info(
        s"${traceId.asString} | Deleting inactive zombie runtime: ${zombie.googleProject.value} / ${zombie.runtimeName.asString}"
      )
      _ <- metrics.incrementCounter("numOfInactiveZombieRuntimes")
      _ <- zombie.cloudService.interpreter
        .deleteRuntime(
          DeleteRuntimeParams(
            zombie.googleProject,
            zombie.runtimeName,
            zombie.asyncRuntimeFields.isDefined,
            None
          ) //TODO: think about this a bit more. We may want to delete disks in certain cases
        )
        .void
        .recoverWith {
          case e =>
            logger
              .warn(e)(
                s"Unable to delete inactive zombie runtime ${zombie.googleProject.value} / ${zombie.runtimeName.asString}"
              )
        }
      // In the next pass of the zombie monitor, this runtime will be marked as confirmed deleted if this succeeded
    } yield ()
}

object ZombieRuntimeMonitor {
  def apply[F[_]: Parallel: ContextShift: Timer](
    config: ZombieRuntimeMonitorConfig,
    googleProjectDAO: GoogleProjectDAO
  )(implicit F: Concurrent[F],
    metrics: OpenTelemetryMetrics[F],
    logger: Logger[F],
    dbRef: DbReference[F],
    ec: ExecutionContext,
    cs: ContextShift[IO],
    runtimes: RuntimeInstances[F]): ZombieRuntimeMonitor[F] =
    new ZombieRuntimeMonitor(config, googleProjectDAO)
}
