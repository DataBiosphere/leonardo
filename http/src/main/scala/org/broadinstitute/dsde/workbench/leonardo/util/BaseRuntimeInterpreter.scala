package org.broadinstitute.dsde.workbench.leonardo.util

import java.text.SimpleDateFormat
import java.time.Instant

import cats.effect.{Async, Blocker, ContextShift}
import cats.implicits._
import cats.mtl.ApplicativeAsk
import io.chrisdavenport.log4cats.Logger
import org.broadinstitute.dsde.workbench.google2.MachineTypeName
import org.broadinstitute.dsde.workbench.leonardo.RuntimeImageType.Welder
import org.broadinstitute.dsde.workbench.leonardo.WelderAction._
import org.broadinstitute.dsde.workbench.leonardo.dao.WelderDAO
import org.broadinstitute.dsde.workbench.leonardo.db.{clusterQuery, labelQuery, DbReference, RuntimeConfigQueries}
import org.broadinstitute.dsde.workbench.leonardo.http._
import org.broadinstitute.dsde.workbench.leonardo.http.service.{
  RuntimeCannotBeStartedException,
  RuntimeCannotBeStoppedException
}
import org.broadinstitute.dsde.workbench.leonardo.{
  Runtime,
  RuntimeConfig,
  RuntimeImage,
  RuntimeOperation,
  RuntimeStatus,
  WelderAction
}
import org.broadinstitute.dsde.workbench.model.TraceId
import org.broadinstitute.dsde.workbench.newrelic.NewRelicMetrics

import scala.concurrent.ExecutionContext
import scala.util.Try

abstract private[util] class BaseRuntimeInterpreter[F[_]: Async: ContextShift: Logger](
  config: RuntimeInterpreterConfig,
  welderDao: WelderDAO[F]
)(implicit dbRef: DbReference[F], metrics: NewRelicMetrics[F], executionContext: ExecutionContext)
    extends RuntimeAlgebra[F] {

  protected def stopGoogleRuntime(runtime: Runtime, runtimeConfig: RuntimeConfig)(
    implicit ev: ApplicativeAsk[F, TraceId]
  ): F[Unit]

  protected def startGoogleRuntime(runtime: Runtime, welderAction: Option[WelderAction], runtimeConfig: RuntimeConfig)(
    implicit ev: ApplicativeAsk[F, TraceId]
  ): F[Unit]

  protected def setMachineTypeInGoogle(runtime: Runtime, machineType: MachineTypeName)(
    implicit ev: ApplicativeAsk[F, TraceId]
  ): F[Unit]

  final override def stopRuntime(params: StopRuntimeParams)(implicit ev: ApplicativeAsk[F, TraceId]): F[Unit] =
    if (params.runtimeAndRuntimeConfig.runtime.status.isStoppable) {
      for {
        // Flush the welder cache to disk
        _ <- if (params.runtimeAndRuntimeConfig.runtime.welderEnabled) {
          welderDao
            .flushCache(params.runtimeAndRuntimeConfig.runtime.googleProject,
                        params.runtimeAndRuntimeConfig.runtime.runtimeName)
            .handleErrorWith(
              e =>
                Logger[F].error(e)(
                  s"Failed to flush welder cache for ${params.runtimeAndRuntimeConfig.runtime.projectNameString}"
                )
            )
        } else Async[F].unit

        // Stop the cluster in Google
        _ <- stopGoogleRuntime(params.runtimeAndRuntimeConfig.runtime, params.runtimeAndRuntimeConfig.runtimeConfig)

        // Update the cluster status to Stopping
        _ <- dbRef.inTransaction { clusterQuery.setToStopping(params.runtimeAndRuntimeConfig.runtime.id, params.now) }
      } yield ()

    } else
      Async[F].raiseError(
        RuntimeCannotBeStoppedException(params.runtimeAndRuntimeConfig.runtime.googleProject,
                                        params.runtimeAndRuntimeConfig.runtime.runtimeName,
                                        params.runtimeAndRuntimeConfig.runtime.status)
      )

  final override def startRuntime(params: StartRuntimeParams)(implicit ev: ApplicativeAsk[F, TraceId]): F[Unit] =
    if (params.runtime.status.isStartable) {
      val welderAction = getWelderAction(params.runtime)
      for {
        // Check if welder should be deployed or updated
        updatedRuntime <- getWelderAction(params.runtime)
          .traverse {
            case DeployWelder | UpdateWelder => updateWelder(params.runtime, params.now)
            case DisableDelocalization =>
              labelQuery.save(params.runtime.id, "welderInstallFailed", "true").transaction.as(params.runtime)
          }
          .map(_.getOrElse(params.runtime))

        runtimeConfig <- dbRef.inTransaction(
          RuntimeConfigQueries.getRuntimeConfig(params.runtime.runtimeConfigId)
        )
        // Start the cluster in Google
        _ <- startGoogleRuntime(updatedRuntime, welderAction, runtimeConfig)

        // Update the cluster status to Starting
        _ <- dbRef.inTransaction {
          clusterQuery.updateClusterStatus(updatedRuntime.id, RuntimeStatus.Starting, params.now)
        }
      } yield ()
    } else
      Async[F].raiseError(
        RuntimeCannotBeStartedException(params.runtime.googleProject, params.runtime.runtimeName, params.runtime.status)
      )

  private def getWelderAction(runtime: Runtime): Option[WelderAction] =
    if (runtime.welderEnabled) {
      // Welder is already enabled; do we need to update it?
      val labelFound = config.welderConfig.updateWelderLabel.exists(runtime.labels.contains)

      val imageChanged = runtime.runtimeImages.find(_.imageType == Welder) match {
        case Some(welderImage) if welderImage.imageUrl != config.imageConfig.welderImage => true
        case _                                                                           => false
      }

      if (labelFound && imageChanged) Some(UpdateWelder)
      else None
    } else {
      // Welder is not enabled; do we need to deploy it?
      val labelFound = config.welderConfig.deployWelderLabel.exists(runtime.labels.contains)
      if (labelFound) {
        if (isClusterBeforeCutoffDate(runtime)) Some(DisableDelocalization)
        else Some(DeployWelder)
      } else None
    }

  private def isClusterBeforeCutoffDate(runtime: Runtime): Boolean =
    (for {
      dateStr <- config.welderConfig.deployWelderCutoffDate
      date <- Try(new SimpleDateFormat("yyyy-MM-dd").parse(dateStr)).toOption
      isClusterBeforeCutoffDate = runtime.auditInfo.createdDate.isBefore(date.toInstant)
    } yield isClusterBeforeCutoffDate) getOrElse false

  private def updateWelder(runtime: Runtime, now: Instant): F[Runtime] =
    for {
      _ <- Logger[F].info(s"Will deploy welder to cluster ${runtime.projectNameString}")
      _ <- metrics.incrementCounter("welder/deploy")
      welderImage = RuntimeImage(Welder, config.imageConfig.welderImage.imageUrl, now)

      _ <- dbRef.inTransaction {
        clusterQuery.updateWelder(runtime.id, RuntimeImage(Welder, config.imageConfig.welderImage.imageUrl, now), now)
      }

      newRuntime = runtime.copy(welderEnabled = true,
                                runtimeImages = runtime.runtimeImages.filterNot(_.imageType == Welder) + welderImage)
    } yield newRuntime

  override def updateMachineType(params: UpdateMachineTypeParams)(implicit ev: ApplicativeAsk[F, TraceId]): F[Unit] =
    for {
      _ <- Logger[F].info(
        s"New machine config present. Changing machine type to ${params.machineType} for cluster ${params.runtime.projectNameString}..."
      )
      // Update the machine type in Google
      _ <- setMachineTypeInGoogle(params.runtime, params.machineType)
      // Update the DB
      _ <- dbRef.inTransaction {
        RuntimeConfigQueries.updateMachineType(params.runtime.runtimeConfigId, params.machineType, params.now)
      }
    } yield ()

  // Startup script to run after the runtime is resumed
  protected def getStartupScript(runtime: Runtime,
                                 welderAction: Option[WelderAction],
                                 blocker: Blocker): F[Map[String, String]] = {
    val googleKey = "startup-script" // required; see https://cloud.google.com/compute/docs/startupscript

    val templateConfig = RuntimeTemplateValuesConfig.fromRuntime(
      runtime,
      None,
      None,
      config.imageConfig,
      config.welderConfig,
      config.proxyConfig,
      config.clusterFilesConfig,
      config.clusterResourcesConfig,
      None,
      RuntimeOperation.Restarting,
      welderAction
    )
    val replacements = RuntimeTemplateValues(templateConfig).toMap

    TemplateHelper
      .templateResource[F](replacements, config.clusterResourcesConfig.startupScript, blocker)
      .through(fs2.text.utf8Decode)
      .compile
      .string
      .map { s =>
        Map(googleKey -> s)
      }
  }

  // Shutdown script to run after the runtime is paused
  protected def getShutdownScript(runtime: Runtime, blocker: Blocker): F[Map[String, String]] = {
    val googleKey = "shutdown-script" // required; see https://cloud.google.com/compute/docs/shutdownscript

    val templateConfig = RuntimeTemplateValuesConfig.fromRuntime(
      runtime,
      None,
      None,
      config.imageConfig,
      config.welderConfig,
      config.proxyConfig,
      config.clusterFilesConfig,
      config.clusterResourcesConfig,
      None,
      RuntimeOperation.Stopping,
      None
    )
    val replacements = RuntimeTemplateValues(templateConfig).toMap

    TemplateHelper
      .templateResource[F](replacements, config.clusterResourcesConfig.shutdownScript, blocker)
      .through(fs2.text.utf8Decode)
      .compile
      .string
      .map { s =>
        Map(googleKey -> s)
      }
  }

}
