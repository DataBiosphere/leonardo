package org.broadinstitute.dsde.workbench.leonardo
package monitor

import java.time.Instant
import java.util.UUID

import cats.effect.{Concurrent, ContextShift, Timer}
import cats.implicits._
import fs2.Stream
import fs2.concurrent.InspectableQueue
import io.chrisdavenport.log4cats.Logger
import org.broadinstitute.dsde.workbench.leonardo.config.AutoFreezeConfig
import org.broadinstitute.dsde.workbench.leonardo.dao.{GalaxyDAO, JupyterDAO}
import org.broadinstitute.dsde.workbench.leonardo.db._
import org.broadinstitute.dsde.workbench.leonardo.http.dbioToIO
import org.broadinstitute.dsde.workbench.leonardo.monitor.LeoPubsubMessage.StopAppMessage
import org.broadinstitute.dsde.workbench.model.TraceId
import org.broadinstitute.dsde.workbench.model.google.GoogleProject
import org.broadinstitute.dsde.workbench.openTelemetry.OpenTelemetryMetrics

import scala.concurrent.ExecutionContext

/**
 * This monitor periodically sweeps the Leo database and auto pause clusters that have been running for too long.
 */
class AutopauseMonitor[F[_]: ContextShift: Timer](
  config: AutoFreezeConfig,
  jupyterDAO: JupyterDAO[F],
  publisherQueue: InspectableQueue[F, LeoPubsubMessage]
)(implicit F: Concurrent[F],
  metrics: OpenTelemetryMetrics[F],
  logger: Logger[F],
  dbRef: DbReference[F],
  ec: ExecutionContext) {

  val process = {
    val processRuntimes =
      if (runtimeAutoFreezeConfig.enableAutoFreeze)
        (Stream.sleep[F](runtimeAutoFreezeConfig.autoFreezeCheckInterval) ++ Stream.eval(runtimeAutoPauseCheck)).repeat
      else Stream.empty
    val processApps =
      if (kubernetesAutoFreezeConfig.enableAutoFreeze)
        (Stream.sleep[F](kubernetesAutoFreezeConfig.autoFreezeCheckInterval) ++ Stream.eval(appAutoPauseCheck)).repeat
      else Stream.empty

    Stream.emits(List(processRuntimes, processApps)).covary[F].parJoin(2)
  }

  private val runtimeAutoPauseCheck: F[Unit] =
    for {
      runtimes <- clusterQuery.getClustersReadyToAutoFreeze.transaction
      now <- nowInstant
      pauseableRuntimes <- runtimes.toList.filterA { runtime =>
        // No kernel check for RStudio
        if (runtime.runtimeImages.map(_.imageType).contains(RuntimeImageType.Jupyter)) {
          checkJupyterKernel(runtime, now)
        } else F.pure(true)
      }
      _ <- metrics.gauge("autoPause/numOfRuntimes", pauseableRuntimes.length)
      _ <- pauseableRuntimes.traverse_ { cl =>
        val traceId = TraceId(s"fromAutopause_${UUID.randomUUID().toString}")
        for {
          _ <- clusterQuery.updateClusterStatus(cl.id, RuntimeStatus.PreStopping, now).transaction
          _ <- logger.info(s"${traceId.asString} | Auto freezing runtime ${cl.projectNameString}")
          _ <- publisherQueue.enqueue1(LeoPubsubMessage.StopRuntimeMessage(cl.id, Some(traceId)))
        } yield ()
      }
    } yield ()

  private val appAutoPauseCheck: F[Unit] = for {
    clusters <- KubernetesServiceDbQueries.getAppsReadyToAutopause.transaction
    now <- nowInstant[F]
    candidates = for {
      c <- clusters
      n <- c.nodepools
      a <- n.apps
    } yield AutopauseAppCandidate(a.id, c.googleProject, a.appName, a.appType, a.foundBusyDate)
    pausableApps <- candidates.filterA { c =>
      c.appType match {
        case AppType.Galaxy => checkGalaxyJobs(c, now)
        case _              => F.pure(true)
      }
    }
    _ <- metrics.gauge("autoPause/numOfApps", pausableApps.length)
    _ <- pausableApps.traverse_ { a =>
      val traceId = TraceId(s"fromAutopause_${UUID.randomUUID().toString}")
      for {
        _ <- logger.info(s"${traceId.asString} | Auto freezing app ${a.id}")
        _ <- appQuery.updateStatus(a.id, AppStatus.PreStopping).transaction
        message = StopAppMessage(
          a.id,
          a.name,
          a.project,
          Some(traceId)
        )
        _ <- publisherQueue.enqueue1(message)
      } yield ()
    }
  } yield ()

  private def checkJupyterKernel(runtime: Runtime, now: Instant): F[Boolean] =
    jupyterDAO.isAllKernelsIdle(runtime.googleProject, runtime.runtimeName).attempt.flatMap {
      case Left(t) =>
        logger.error(s"Fail to get kernel status for ${runtime.projectNameString} due to $t").as(true)
      case Right(true) => F.pure(true)
      case Right(false) =>
        val maxKernelActiveTimeExceeded = runtime.kernelFoundBusyDate match {
          case Some(attemptedDate) =>
            val maxBusyLimitReached =
              now.toEpochMilli - attemptedDate.toEpochMilli > runtimeAutoFreezeConfig.maxBusyLimit.toMillis
            F.pure(maxBusyLimitReached)
          case None =>
            clusterQuery
              .updateKernelFoundBusyDate(runtime.id, now, now)
              .transaction
              .as(false) // max kernel active time has not been exceeded
        }

        maxKernelActiveTimeExceeded.ifM(
          metrics.incrementCounter("autoPause/maxKernelActiveTimeExceeded") >>
            logger
              .info(
                s"Auto pausing ${runtime.projectNameString} due to exceeded max kernel active time"
              )
              .as(true),
          metrics.incrementCounter("autoPause/activeKernelClusters") >>
            logger
              .info(
                s"Not going to auto pause cluster ${runtime.projectNameString} due to active kernels"
              )
              .as(false)
        )
    }

  private def checkGalaxyJobs(candidate: AutopauseAppCandidate, now: Instant): F[Boolean] =
    galaxyDAO.hasJobsRunning(candidate.project, candidate.name).attempt.flatMap {
      case Left(t) =>
        logger.error(s"Fail to get Galaxy job status for ${candidate.id} due to $t").as(true)
      case Right(true) => F.pure(true)
      case Right(false) =>
        val maxyBusyTimeExceeded = candidate.foundBusyDate match {
          case Some(attemptedDate) =>
            val maxBusyLimitReached =
              now.toEpochMilli - attemptedDate.toEpochMilli > kubernetesAutoFreezeConfig.maxBusyLimit.toMillis
            F.pure(maxBusyLimitReached)
          case None =>
            appQuery.updateFoundBusyDate(candidate.id, now).transaction.as(false)
        }

        maxyBusyTimeExceeded.ifM(
          metrics.incrementCounter("autoPause/maxGalaxyActiveTimeExceeded") >>
            logger
              .info(
                s"Auto pausing app ${candidate.id} due to exceeded Galaxy active time"
              )
              .as(true),
          metrics.incrementCounter("autoPause/activeGalaxyApps") >>
            logger
              .info(
                s"Not going to auto pause app ${candidate.id} due to active Galaxy jobs"
              )
              .as(false)
        )
    }
}

case class AutopauseAppCandidate(id: AppId,
                                 project: GoogleProject,
                                 name: AppName,
                                 appType: AppType,
                                 foundBusyDate: Option[Instant])
