package org.broadinstitute.dsde.workbench.leonardo
package monitor

import java.util.concurrent.TimeUnit

import cats.Parallel
import cats.effect.concurrent.Semaphore
import cats.effect.{Concurrent, ContextShift, Timer}
import cats.implicits._
import cats.mtl.Ask
import fs2.Stream
import io.chrisdavenport.log4cats.Logger
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
 * Inactive zombie : a runtime that is in Deleted status in the Leo DB, but still running in Google
 */
class ZombieRuntimeMonitor[F[_]: Parallel: ContextShift: Timer](
  config: ZombieRuntimeMonitorConfig
)(implicit F: Concurrent[F],
  metrics: OpenTelemetryMetrics[F],
  logger: Logger[F],
  dbRef: DbReference[F],
  ec: ExecutionContext,
  runtimes: RuntimeInstances[F]) {

  val process: Stream[F, Unit] =
    (Stream.sleep[F](config.zombieCheckPeriod) ++ Stream.eval(
      zombieCheck
        .handleErrorWith(e => logger.error(e)("Unexpected error occurred during zombie monitoring"))
    )).repeat

  private[monitor] val zombieCheck: F[Unit] =
    for {
      start <- Timer[F].clock.realTime(TimeUnit.MILLISECONDS)
      tid = TraceId(s"fromZombieCheck_${start}")
      implicit0(traceId: Ask[F, TraceId]) = Ask.const[F, TraceId](tid)
      semaphore <- Semaphore[F](config.concurrency)
      // Get all deleted runtimes that haven't been confirmed from the Leo DB
      unconfirmedDeletedRuntimes <- ZombieMonitorQueries.listInactiveZombieQuery.transaction

      _ <- logger.info(
        s"Starting inactive zombie detection within ${unconfirmedDeletedRuntimes.size} runtimes with concurrency of ${config.concurrency}"
      )

      inactiveZombies <- unconfirmedDeletedRuntimes
        .parTraverse[F, Option[ZombieCandidate]] { candidate =>
          semaphore.withPermit {
            ifRuntimeExistInGoogle(candidate.googleProject, candidate.runtimeName, candidate.cloudService).attempt
              .flatMap {
                case Left(e) =>
                  logger
                    .warn(e)(
                      s"Unable to check status of runtime ${candidate.googleProject.value} / ${candidate.runtimeName.asString} for inactive zombie runtime detection"
                    )
                    .as(None)
                case Right(true) =>
                  // it's an inactive zombie if it exists in Google
                  F.pure(Some(candidate))
                case Right(false) =>
                  updateRuntimeAsConfirmedDeleted(candidate.id).as(None)
              }
          }
        }
        .map(_.flattenOption)

      // Delete runtime on Leo side if active zombie, or on Google side if inactive zombie
      _ <- inactiveZombies.parTraverse(zombie => semaphore.withPermit(handleInactiveZombieRuntime(zombie)))
      end <- Timer[F].clock.realTime(TimeUnit.MILLISECONDS)
      duration = end - start
      _ <- logger.info(
        s"Detected ${inactiveZombies.size} inactive zombie runtimes. " +
          s"Elapsed time = ${duration} milli seconds."
      )
    } yield ()

  // Puts a label on a deleted runtime to show that we've confirmed the google runtime's deletion
  private def updateRuntimeAsConfirmedDeleted(runtimeId: Long): F[Unit] =
    dbRef.inTransaction {
      labelQuery.save(runtimeId, LabelResourceType.Runtime, config.deletionConfirmationLabelKey, "true")
    }.void

  private[monitor] def ifRuntimeExistInGoogle(
    googleProject: GoogleProject,
    runtimeName: RuntimeName,
    cloudService: CloudService
  )(implicit traceId: Ask[F, TraceId]): F[Boolean] =
    cloudService.interpreter
      .getRuntimeStatus(GetRuntimeStatusParams(googleProject, runtimeName, Some(config.gceZoneName)))
      .map(s => s != RuntimeStatus.Deleted)

  private def handleInactiveZombieRuntime(zombie: ZombieCandidate)(implicit ev: Ask[F, TraceId]): F[Unit] =
    for {
      traceId <- ev.ask
      _ <- logger.info(
        s"${traceId.asString} | Deleting inactive zombie runtime: ${zombie.googleProject.value} / ${zombie.runtimeName.asString}"
      )
      _ <- metrics.incrementCounter("numOfInactiveZombieRuntimes")
      runtimeOpt <- clusterQuery.getClusterById(zombie.id).transaction
      runtime <- F.fromOption(
        runtimeOpt,
        new RuntimeException(
          s"Inactive zombine runtime ${zombie.googleProject.value} / ${zombie.runtimeName.asString} not found in Leo DB | trace id: ${traceId}"
        )
      )
      //TODO: think about this a bit more. We may want to delete disks in certain cases
      _ <- zombie.cloudService.interpreter
        .deleteRuntime(DeleteRuntimeParams(runtime))
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
    config: ZombieRuntimeMonitorConfig
  )(implicit F: Concurrent[F],
    metrics: OpenTelemetryMetrics[F],
    logger: Logger[F],
    dbRef: DbReference[F],
    ec: ExecutionContext,
    runtimes: RuntimeInstances[F]): ZombieRuntimeMonitor[F] =
    new ZombieRuntimeMonitor(config)
}
