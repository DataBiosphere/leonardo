package org.broadinstitute.dsde.workbench.leonardo
package util

import java.text.SimpleDateFormat
import java.time.Instant

import cats.effect.{Async, Blocker, ContextShift}
import cats.mtl.Ask
import cats.syntax.all._
import com.google.cloud.compute.v1.Operation
import io.chrisdavenport.log4cats.StructuredLogger
import org.broadinstitute.dsde.workbench.google2.MachineTypeName
import org.broadinstitute.dsde.workbench.leonardo.RuntimeImageType.Welder
import org.broadinstitute.dsde.workbench.leonardo.WelderAction._
import org.broadinstitute.dsde.workbench.leonardo.dao.WelderDAO
import org.broadinstitute.dsde.workbench.leonardo.db._
import org.broadinstitute.dsde.workbench.leonardo.http._
import org.broadinstitute.dsde.workbench.model.google.GcsBucketName
import org.broadinstitute.dsde.workbench.openTelemetry.OpenTelemetryMetrics

import scala.concurrent.ExecutionContext
import scala.util.Try

abstract private[util] class BaseRuntimeInterpreter[F[_]: ContextShift](
  config: RuntimeInterpreterConfig,
  welderDao: WelderDAO[F]
)(implicit F: Async[F],
  dbRef: DbReference[F],
  metrics: OpenTelemetryMetrics[F],
  logger: StructuredLogger[F],
  executionContext: ExecutionContext)
    extends RuntimeAlgebra[F] {

  protected def stopGoogleRuntime(runtime: Runtime, dataprocConfig: Option[RuntimeConfig.DataprocConfig])(
    implicit ev: Ask[F, AppContext]
  ): F[Option[Operation]]

  protected def startGoogleRuntime(params: StartGoogleRuntime)(
    implicit ev: Ask[F, AppContext]
  ): F[Unit]

  protected def setMachineTypeInGoogle(runtime: Runtime, machineType: MachineTypeName)(
    implicit ev: Ask[F, AppContext]
  ): F[Unit]

  final override def stopRuntime(
    params: StopRuntimeParams
  )(implicit ev: Ask[F, AppContext]): F[Option[Operation]] =
    for {
      ctx <- ev.ask
      // Flush the welder cache to disk
      _ <- if (params.runtime.welderEnabled) {
        welderDao
          .flushCache(params.runtime.googleProject, params.runtime.runtimeName)
          .handleErrorWith(e =>
            logger.error(ctx.loggingCtx, e)(
              s"Failed to flush welder cache for ${params.runtime.projectNameString}"
            )
          )
      } else F.unit

      _ <- clusterQuery.updateClusterHostIp(params.runtime.id, None, ctx.now).transaction

      // Stop the cluster in Google
      r <- stopGoogleRuntime(params.runtime, params.dataprocConfig)
    } yield r

  final override def startRuntime(params: StartRuntimeParams)(implicit ev: Ask[F, AppContext]): F[Unit] = {
    val welderAction = getWelderAction(params.runtime)
    for {
      ctx <- ev.ask
      // Check if welder should be deployed or updated
      updatedRuntime <- welderAction
        .traverse {
          case UpdateWelder => updateWelder(params.runtime, ctx.now)
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
      startGoogleRuntimeReq = StartGoogleRuntime(updatedRuntime, params.initBucket, welderAction, runtimeConfig)
      // Start the cluster in Google
      _ <- startGoogleRuntime(startGoogleRuntimeReq)
    } yield ()
  }

  private def getWelderAction(runtime: Runtime): Option[WelderAction] =
    if (runtime.welderEnabled) {
      // Welder is already enabled; do we need to update it?
      val labelFound = config.welderConfig.updateWelderLabel.exists(runtime.labels.contains)

      val imageChanged = runtime.runtimeImages.find(_.imageType == Welder) match {
        case Some(welderImage) if welderImage.hash != Some(config.imageConfig.welderHash) => true
        case _                                                                            => false
      }

      if (labelFound && imageChanged) Some(UpdateWelder)
      else None
    } else {
      // Welder is not enabled; do we need to deploy it?
      val labelFound = config.welderConfig.deployWelderLabel.exists(runtime.labels.contains)
      if (labelFound) {
        if (isClusterBeforeCutoffDate(runtime)) Some(DisableDelocalization)
        else None
      } else None
    }

  private def isClusterBeforeCutoffDate(runtime: Runtime): Boolean =
    (for {
      dateStr <- config.welderConfig.deployWelderCutoffDate
      date <- Try(new SimpleDateFormat("yyyy-MM-dd").parse(dateStr)).toOption
      isClusterBeforeCutoffDate = runtime.auditInfo.createdDate.isBefore(date.toInstant)
    } yield isClusterBeforeCutoffDate) getOrElse false

  private def updateWelder(runtime: Runtime, now: Instant)(implicit ev: Ask[F, AppContext]): F[Runtime] =
    for {
      ctx <- ev.ask
      _ <- logger.info(ctx.loggingCtx)(s"Will deploy welder to runtime ${runtime.projectNameString}")
      _ <- metrics.incrementCounter("welder/upgrade")

      newWelderImageUrl <- Async[F].fromEither(
        runtime.runtimeImages
          .find(_.imageType == Welder)
          .toRight(new Exception(s"Unable to update welder because current welder image is not available"))
          .flatMap(x =>
            x.registry match {
              case Some(ContainerRegistry.GCR) | Some(ContainerRegistry.GHCR) =>
                Right(config.imageConfig.welderGcrImage.imageUrl)
              case Some(ContainerRegistry.DockerHub) => Right(config.imageConfig.welderDockerHubImage.imageUrl)
              case None                              => Left(new Exception(s"Unable to update Welder: registry for ${x.imageUrl} not parsable"))
            }
          )
      )
      welderImage = RuntimeImage(Welder, newWelderImageUrl, now)

      _ <- dbRef.inTransaction {
        clusterQuery.updateWelder(runtime.id, RuntimeImage(Welder, newWelderImageUrl, now), now)
      }

      newRuntime = runtime.copy(welderEnabled = true,
                                runtimeImages = runtime.runtimeImages.filterNot(_.imageType == Welder) + welderImage)
    } yield newRuntime

  override def updateMachineType(params: UpdateMachineTypeParams)(implicit ev: Ask[F, AppContext]): F[Unit] =
    for {
      ctx <- ev.ask
      _ <- logger.info(ctx.loggingCtx)(
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
                                 initBucket: GcsBucketName,
                                 blocker: Blocker,
                                 runtimeResourceConstraints: RuntimeResourceConstraints,
                                 useGceStartupScript: Boolean)(
    implicit ev: Ask[F, AppContext]
  ): F[Map[String, String]] = {
    val googleKey = "startup-script" // required; see https://cloud.google.com/compute/docs/startupscript

    val templateConfig = RuntimeTemplateValuesConfig.fromRuntime(
      runtime,
      Some(initBucket),
      None,
      config.imageConfig,
      config.welderConfig,
      config.proxyConfig,
      config.clusterFilesConfig,
      config.clusterResourcesConfig,
      Some(runtimeResourceConstraints),
      RuntimeOperation.Restarting,
      welderAction,
      useGceStartupScript
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
      None,
      false
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

final case class StartGoogleRuntime(runtime: Runtime,
                                    initBucket: GcsBucketName,
                                    welderAction: Option[WelderAction],
                                    runtimeConfig: RuntimeConfig)
