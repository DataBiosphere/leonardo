package org.broadinstitute.dsde.workbench.leonardo
package monitor

import java.time.Instant
import java.util.concurrent.TimeUnit

import cats.effect.{Async, Timer}
import cats.implicits._
import cats.mtl.ApplicativeAsk
import com.google.cloud.compute.v1.Instance
import fs2.Stream
import org.broadinstitute.dsde.workbench.google2.{RegionName, ZoneName}
import org.broadinstitute.dsde.workbench.leonardo.http.userScriptStartupOutputUriMetadataKey
import org.broadinstitute.dsde.workbench.leonardo.config.{Config, ImageConfig, RuntimeBucketConfig}
import org.broadinstitute.dsde.workbench.model.TraceId
import org.broadinstitute.dsde.workbench.model.google.{GcsPath, GoogleProject}
import org.broadinstitute.dsde.workbench.openTelemetry.OpenTelemetryMetrics
import scala.collection.JavaConverters._

import scala.collection.immutable.Set
import scala.concurrent.duration._

/**
 * Monitor for runtime status transition: Starting, Creating, Deleting, Stopping
 * It doesn't trigger any of the action but only responsible for monitoring the progress and make necessary cleanup when the transition is done
 */
trait RuntimeMonitor[F[_], A] {
  def process(a: A)(runtimeId: Long, action: RuntimeStatus)(implicit ev: ApplicativeAsk[F, TraceId]): Stream[F, Unit]

  // Function used for transitions that we can get an Operation
  def pollCheck(a: A)(googleProject: GoogleProject,
                      runtimeAndRuntimeConfig: RuntimeAndRuntimeConfig,
                      operation: com.google.cloud.compute.v1.Operation,
                      action: RuntimeStatus)(implicit ev: ApplicativeAsk[F, TraceId]): F[Unit]
}

object RuntimeMonitor {
  type CheckResult = (Unit, Option[MonitorState])

  def getRuntimeUI(runtime: Runtime): RuntimeUI =
    if (runtime.labels.contains(Config.uiConfig.terraLabel)) RuntimeUI.Terra
    else if (runtime.labels.contains(Config.uiConfig.allOfUsLabel)) RuntimeUI.AoU
    else RuntimeUI.Other

  private[monitor] def recordStatusTransitionMetrics[F[_]: Timer: Async](
    startTime: Instant,
    runtimeUI: RuntimeUI,
    origStatus: RuntimeStatus,
    finalStatus: RuntimeStatus,
    cloudService: CloudService
  )(implicit openTelemetry: OpenTelemetryMetrics[F]): F[Unit] =
    for {
      endTime <- Timer[F].clock.realTime(TimeUnit.MILLISECONDS)
      metricsName = s"monitor/transition/${origStatus}_to_${finalStatus}"
      duration = (endTime - startTime.toEpochMilli).millis
      tags = Map("cloudService" -> cloudService.asString, "ui_client" -> runtimeUI.asString)
      _ <- openTelemetry.incrementCounter(metricsName, 1, tags)
      distributionBucket = List(0.5 minutes,
                                1 minutes,
                                1.5 minutes,
                                2 minutes,
                                2.5 minutes,
                                3 minutes,
                                3.5 minutes,
                                4 minutes,
                                4.5 minutes) //Distribution buckets from 0.5 min to 4.5 min
      _ <- openTelemetry.recordDuration(metricsName, duration, distributionBucket, tags)
    } yield ()

  private def findToolImageInfo(images: Set[RuntimeImage], imageConfig: ImageConfig): String = {
    val terraJupyterImage = imageConfig.jupyterImageRegex.r
    val anvilRStudioImage = imageConfig.rstudioImageRegex.r
    val broadDockerhubImageRegex = imageConfig.broadDockerhubImageRegex.r
    images.find(runtimeImage => Set(RuntimeImageType.Jupyter, RuntimeImageType.RStudio) contains runtimeImage.imageType) match {
      case Some(toolImage) =>
        toolImage.imageUrl match {
          case terraJupyterImage(imageType, hash)        => s"GCR/${imageType}/${hash}"
          case anvilRStudioImage(imageType, hash)        => s"GCR/${imageType}/${hash}"
          case broadDockerhubImageRegex(imageType, hash) => s"DockerHub/${imageType}/${hash}"
          case _                                         => "custom_image"
        }
      case None => "unknown"
    }
  }

  private[monitor] def recordClusterCreationMetrics[F[_]: Timer: Async](
    createdDate: Instant,
    images: Set[RuntimeImage],
    imageConfig: ImageConfig,
    cloudService: CloudService
  )(implicit openTelemetry: OpenTelemetryMetrics[F]): F[Unit] =
    for {
      endTime <- Timer[F].clock.realTime(TimeUnit.MILLISECONDS)
      toolImageInfo = findToolImageInfo(images, imageConfig)
      metricsName = s"monitor/runtimeCreation"
      duration = (endTime - createdDate.toEpochMilli).milliseconds
      tags = Map("cloudService" -> cloudService.asString, "image" -> toolImageInfo)
      _ <- openTelemetry.incrementCounter(metricsName, 1, tags)
      distributionBucket = List(1 minutes,
                                1.5 minutes,
                                2 minutes,
                                2.5 minutes,
                                3 minutes,
                                3.5 minutes,
                                4 minutes,
                                4.5 minutes,
                                5 minutes,
                                5.5 minutes,
                                6 minutes) //Distribution buckets from 1 min to 6 min
      _ <- openTelemetry.recordDuration(metricsName, duration, distributionBucket, tags)
    } yield ()

  def getUserScript(instance: Instance): Option[GcsPath] =
    for {
      metadata <- Option(instance.getMetadata)
      item <- metadata.getItemsList.asScala.toList
        .filter(item => item.getKey == userScriptStartupOutputUriMetadataKey)
        .headOption
      s <- org.broadinstitute.dsde.workbench.model.google.parseGcsPath(item.getValue).toOption
    } yield s
}

final case class MonitorContext(start: Instant, runtimeId: Long, traceId: TraceId, action: RuntimeStatus) {
  override def toString: String = s"${runtimeId}/${traceId.asString}"
}

sealed abstract class MonitorState extends Product with Serializable
object MonitorState {
  final case object Initial extends MonitorState
  final case class Check(runtimeAndRuntimeConfig: RuntimeAndRuntimeConfig) extends MonitorState
}

sealed trait MonitorConfig {
  def initialDelay: FiniteDuration
  def pollingInterval: FiniteDuration
  def imageConfig: ImageConfig
  def monitorStatusTimeouts: Map[RuntimeStatus, FiniteDuration]
}

object MonitorConfig {
  final case class GceMonitorConfig(initialDelay: FiniteDuration,
                                    pollingInterval: FiniteDuration,
                                    pollCheckMaxAttempts: Int,
                                    checkToolsDelay: FiniteDuration,
                                    runtimeBucketConfig: RuntimeBucketConfig,
                                    monitorStatusTimeouts: Map[RuntimeStatus, FiniteDuration],
                                    gceZoneName: ZoneName,
                                    imageConfig: ImageConfig)
      extends MonitorConfig

  final case class DataprocMonitorConfig(initialDelay: FiniteDuration,
                                         pollingInterval: FiniteDuration,
                                         pollCheckMaxAttempts: Int,
                                         checkToolsDelay: FiniteDuration,
                                         runtimeBucketConfig: RuntimeBucketConfig,
                                         monitorStatusTimeouts: Map[RuntimeStatus, FiniteDuration],
                                         imageConfig: ImageConfig,
                                         regionName: RegionName)
      extends MonitorConfig
}
