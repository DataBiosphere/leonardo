package org.broadinstitute.dsde.workbench.leonardo
package util

import java.text.SimpleDateFormat
import java.time.Instant

import cats.effect.{Async, Blocker, ContextShift}
import cats.implicits._
import cats.mtl.ApplicativeAsk
import com.google.cloud.compute.v1.Operation
import io.chrisdavenport.log4cats.Logger
import org.broadinstitute.dsde.workbench.google2.MachineTypeName
import org.broadinstitute.dsde.workbench.leonardo.RuntimeImageType.Welder
import org.broadinstitute.dsde.workbench.leonardo.WelderAction._
import org.broadinstitute.dsde.workbench.leonardo.dao.WelderDAO
import org.broadinstitute.dsde.workbench.leonardo.db.{
  clusterQuery,
  labelQuery,
  DbReference,
  LabelResourceType,
  RuntimeConfigQueries
}
import org.broadinstitute.dsde.workbench.leonardo.http._
import org.broadinstitute.dsde.workbench.model.TraceId
import org.broadinstitute.dsde.workbench.openTelemetry.OpenTelemetryMetrics

import scala.concurrent.ExecutionContext
import scala.util.Try

abstract private[util] class BaseRuntimeInterpreter[F[_]: Async: ContextShift: Logger](
  config: RuntimeInterpreterConfig,
  welderDao: WelderDAO[F]
)(implicit dbRef: DbReference[F], metrics: OpenTelemetryMetrics[F], executionContext: ExecutionContext)
    extends RuntimeAlgebra[F] {

  protected def stopGoogleRuntime(runtime: Runtime, runtimeConfig: RuntimeConfig)(
    implicit ev: ApplicativeAsk[F, TraceId]
  ): F[Option[Operation]]

  protected def startGoogleRuntime(runtime: Runtime, welderAction: Option[WelderAction], runtimeConfig: RuntimeConfig)(
    implicit ev: ApplicativeAsk[F, AppContext]
  ): F[Unit]

  protected def setMachineTypeInGoogle(runtime: Runtime, machineType: MachineTypeName)(
    implicit ev: ApplicativeAsk[F, TraceId]
  ): F[Unit]

  final override def stopRuntime(
    params: StopRuntimeParams
  )(implicit ev: ApplicativeAsk[F, AppContext]): F[Option[Operation]] =
    for {
      ctx <- ev.ask
      // Flush the welder cache to disk
      _ <- if (params.runtimeAndRuntimeConfig.runtime.welderEnabled) {
        welderDao
          .flushCache(params.runtimeAndRuntimeConfig.runtime.googleProject,
                      params.runtimeAndRuntimeConfig.runtime.runtimeName)
          .handleErrorWith(e =>
            Logger[F].error(e)(
              s"Failed to flush welder cache for ${params.runtimeAndRuntimeConfig.runtime.projectNameString}"
            )
          )
      } else Async[F].unit

      _ <- clusterQuery.updateClusterHostIp(params.runtimeAndRuntimeConfig.runtime.id, None, ctx.now).transaction

      // Stop the cluster in Google
      r <- stopGoogleRuntime(params.runtimeAndRuntimeConfig.runtime, params.runtimeAndRuntimeConfig.runtimeConfig)
    } yield r

  final override def startRuntime(params: StartRuntimeParams)(implicit ev: ApplicativeAsk[F, AppContext]): F[Unit] = {
    val welderAction = getWelderAction(params.runtime)
    for {
      // Check if welder should be deployed or updated
      updatedRuntime <- welderAction
        .traverse {
          case DeployWelder | UpdateWelder => updateWelder(params.runtime, params.now)
          case DisableDelocalization =>
            labelQuery
              .save(params.runtime.id, LabelResourceType.Runtime, "welderInstallFailed", "true")
              .transaction
              .as(params.runtime)
        }
        .map(_.getOrElse(params.runtime))

      runtimeConfig <- dbRef.inTransaction(
        RuntimeConfigQueries.getRuntimeConfig(params.runtime.runtimeConfigId)
      )
      // Start the cluster in Google
      _ <- startGoogleRuntime(updatedRuntime, welderAction, runtimeConfig)
    } yield ()
  }

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
      _ <- metrics.incrementCounter("welder/upgrade")
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
                                 now: Instant,
                                 blocker: Blocker,
                                 runtimeResourceConstraints: RuntimeResourceConstraints)(
    implicit ev: ApplicativeAsk[F, AppContext]
  ): F[Map[String, String]] = {
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
      Some(runtimeResourceConstraints),
      RuntimeOperation.Restarting,
      welderAction
    )

    for {
      ctx <- ev.ask
      replacements = RuntimeTemplateValues(templateConfig, Some(ctx.now))
      mp <- TemplateHelper
        .templateResource[F](replacements.toMap, config.clusterResourcesConfig.startupScript, blocker)
        .through(fs2.text.utf8Decode)
        .compile
        .string
        .map { s =>
          Map(
            googleKey -> s,
            userScriptStartupOutputUriMetadataKey -> replacements.jupyterStartUserScriptOutputUri
          )
        }
    } yield mp
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
    val replacements = RuntimeTemplateValues(templateConfig, None).toMap

    TemplateHelper
      .templateResource[F](replacements, config.clusterResourcesConfig.shutdownScript, blocker)
      .through(fs2.text.utf8Decode)
      .compile
      .string
      .map(s => Map(googleKey -> s))
  }

}
