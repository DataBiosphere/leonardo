package org.broadinstitute.dsde.workbench.leonardo.util

import java.text.SimpleDateFormat
import java.time.Instant

import cats.effect.{Async, ContextShift}
import cats.implicits._
import cats.mtl.ApplicativeAsk
import io.chrisdavenport.log4cats.Logger
import org.broadinstitute.dsde.workbench.google2.MachineTypeName
import org.broadinstitute.dsde.workbench.leonardo.RuntimeImageType.Welder
import org.broadinstitute.dsde.workbench.leonardo.WelderAction._
import org.broadinstitute.dsde.workbench.leonardo.dao.WelderDAO
import org.broadinstitute.dsde.workbench.leonardo.db.{DbReference, RuntimeConfigQueries, clusterQuery, labelQuery}
import org.broadinstitute.dsde.workbench.leonardo.http.service.{RuntimeCannotBeStartedException, RuntimeCannotBeStoppedException, RuntimeOutOfDateException}
import org.broadinstitute.dsde.workbench.leonardo.{Runtime, RuntimeConfig, RuntimeImage, RuntimeStatus, WelderAction}
import org.broadinstitute.dsde.workbench.model.TraceId
import org.broadinstitute.dsde.workbench.newrelic.NewRelicMetrics

import scala.concurrent.ExecutionContext
import scala.util.Try

private[util] abstract class BaseRuntimeInterpreter[F[_]: Async: ContextShift: Logger](config: RuntimeInterpreterConfig, welderDao: WelderDAO[F])(implicit dbRef: DbReference[F], metrics: NewRelicMetrics[F], executionContext: ExecutionContext) extends RuntimeAlgebra[F] {

  protected def stopGoogleRuntime(runtime: Runtime, runtimeConfig: RuntimeConfig)(implicit ev: ApplicativeAsk[F, TraceId]): F[Unit]

  protected def startGoogleRuntime(runtime: Runtime, welderAction: WelderAction, runtimeConfig: RuntimeConfig)(implicit ev: ApplicativeAsk[F, TraceId]): F[Unit]

  protected def setMachineTypeInGoogle(runtime: Runtime, machineType: MachineTypeName)(implicit ev: ApplicativeAsk[F, TraceId]): F[Unit]

  override final def stopRuntime(params: StopRuntimeParams)(implicit ev: ApplicativeAsk[F, TraceId]): F[Unit] =
    if (params.runtime.status.isStoppable) {
      for {
        // Flush the welder cache to disk
        _ <- if (params.runtime.welderEnabled) {
          welderDao
            .flushCache(params.runtime.googleProject, params.runtime.runtimeName)
            .handleErrorWith(e => Logger[F].error(e)(s"Failed to flush welder cache for ${params.runtime.projectNameString}"))
        } else Async[F].unit

        // Stop the cluster in Google
        _ <- stopGoogleRuntime(params.runtime, params.runtimeConfig)

        // Update the cluster status to Stopping
        now <- Async[F].delay(Instant.now)
        _ <- dbRef.inTransaction { clusterQuery.setToStopping(params.runtime.id, now) }
      } yield ()

    } else Async[F].raiseError(RuntimeCannotBeStoppedException(params.runtime.googleProject, params.runtime.runtimeName, params.runtime.status))

  override final def startRuntime(params: StartRuntimeParams)(implicit ev: ApplicativeAsk[F, TraceId]): F[Unit] =
    if (params.runtime.status.isStartable) {
      val welderAction = getWelderAction(params.runtime)
      for {
        // Check if welder should be deployed or updated
        now <- Async[F].delay(Instant.now)
        updatedRuntime <- welderAction match {
          case DeployWelder | UpdateWelder      => updateWelder(params.runtime, now)
          case NoAction | DisableDelocalization => Async[F].pure(params.runtime)
          case RuntimeOutOfDate                 => Async[F].raiseError[Runtime](RuntimeOutOfDateException())
        }
        _ <- if (welderAction == DisableDelocalization && !params.runtime.labels.contains("welderInstallFailed"))
          dbRef.inTransaction { labelQuery.save(params.runtime.id, "welderInstallFailed", "true") }.void
        else Async[F].unit

        runtimeConfig <- dbRef.inTransaction(
          RuntimeConfigQueries.getRuntimeConfig(params.runtime.runtimeConfigId)
        )
        // Start the cluster in Google
        _ <- startGoogleRuntime(updatedRuntime, welderAction, runtimeConfig)

        // Update the cluster status to Starting
        now <- Async[F].delay(Instant.now)
        _ <- dbRef.inTransaction { clusterQuery.updateClusterStatus(updatedRuntime.id, RuntimeStatus.Starting, now) }
      } yield ()
    } else Async[F].raiseError(RuntimeCannotBeStartedException(params.runtime.googleProject, params.runtime.runtimeName, params.runtime.status))

  private def getWelderAction(runtime: Runtime)(implicit ev: ApplicativeAsk[F, TraceId]): WelderAction =
    if (runtime.welderEnabled) {
      // Welder is already enabled; do we need to update it?
      val labelFound = config.welderConfig.updateWelderLabel.exists(runtime.labels.contains)

      val imageChanged = runtime.runtimeImages.find(_.imageType == Welder) match {
        case Some(welderImage) if welderImage.imageUrl != config.imageConfig.welderImage => true
        case _                                                                    => false
      }

      if (labelFound && imageChanged) UpdateWelder
      else NoAction
    } else {
      // Welder is not enabled; do we need to deploy it?
      val labelFound = config.welderConfig.deployWelderLabel.exists(runtime.labels.contains)
      if (labelFound) {
        if (isClusterBeforeCutoffDate(runtime)) DisableDelocalization
        else DeployWelder
      } else NoAction
    }

  private def isClusterBeforeCutoffDate(runtime: Runtime)(implicit ev: ApplicativeAsk[F, TraceId]): Boolean =
    (for {
      dateStr <- config.welderConfig.deployWelderCutoffDate
      date <- Try(new SimpleDateFormat("yyyy-MM-dd").parse(dateStr)).toOption
      isClusterBeforeCutoffDate = runtime.auditInfo.createdDate.isBefore(date.toInstant)
    } yield isClusterBeforeCutoffDate) getOrElse false

  private def updateWelder(runtime: Runtime, now: Instant)(implicit ev: ApplicativeAsk[F, TraceId]): F[Runtime] =
    for {
      _ <- Logger[F].info(s"Will deploy welder to cluster ${runtime.projectNameString}")
      _ <- metrics.incrementCounter("welder/deploy")
      now <- Async[F].delay(Instant.now)
      welderImage = RuntimeImage(Welder, config.imageConfig.welderImage, now)

      _ <- dbRef.inTransaction {
        clusterQuery.updateWelder(runtime.id, RuntimeImage(Welder, config.imageConfig.welderImage, now), now)
      }

      newRuntime = runtime.copy(welderEnabled = true,
        runtimeImages = runtime.runtimeImages.filterNot(_.imageType == Welder) + welderImage)
    } yield newRuntime

  override def updateMachineType(params: UpdateMachineTypeParams)(implicit ev: ApplicativeAsk[F, TraceId]): F[Unit] =
    for {
      _ <- Logger[F].info(s"New machine config present. Changing machine type to ${params.machineType} for cluster ${params.runtime.projectNameString}...")
      // Update the machine type in Google
      _ <- setMachineTypeInGoogle(params.runtime, params.machineType)
      // Update the DB
      now <- Async[F].pure(Instant.now)
      _ <- dbRef.inTransaction {
        RuntimeConfigQueries.updateMachineType(params.runtime.runtimeConfigId, params.machineType, now)
      }
    } yield ()

}
