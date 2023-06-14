package org.broadinstitute.dsde.workbench.leonardo
package monitor

import cats.effect.Async
import cats.mtl.Ask
import cats.syntax.all._
import com.google.cloud.compute.v1.Instance
import fs2.Stream
import org.broadinstitute.dsde.workbench.leonardo.config.{Config, ImageConfig, RuntimeBucketConfig, VPCConfig}
import org.broadinstitute.dsde.workbench.leonardo.http.userScriptStartupOutputUriMetadataKey
import org.broadinstitute.dsde.workbench.model.TraceId
import org.broadinstitute.dsde.workbench.model.google.GcsPath
import org.broadinstitute.dsde.workbench.openTelemetry.OpenTelemetryMetrics

import java.time.Instant
import scala.collection.immutable.Set
import scala.concurrent.duration._
import scala.jdk.CollectionConverters._

/**
 * Monitor for runtime status transition: Starting, Creating, Deleting, Stopping
 * It doesn't trigger any of the action but only responsible for monitoring the progress and make necessary cleanup when the transition is done
 */
trait RuntimeMonitor[F[_], A] {
  def process(a: A)(runtimeId: Long, action: RuntimeStatus, checkToolsInterruptAfter: Option[FiniteDuration])(implicit
    ev: Ask[F, TraceId]
  ): Stream[F, Unit]

  def handlePollCheckCompletion(
    a: A
  )(monitorContext: MonitorContext, runtimeAndRuntimeConfig: RuntimeAndRuntimeConfig): F[Unit]
}

object RuntimeMonitor {
  type CheckResult = (Unit, Option[MonitorState])

  def getRuntimeUI(runtime: Runtime): RuntimeUI =
    if (runtime.labels.contains(Config.uiConfig.terraLabel)) RuntimeUI.Terra
    else if (runtime.labels.contains(Config.uiConfig.allOfUsLabel)) RuntimeUI.AoU
    else RuntimeUI.Other

  private[monitor] def recordStatusTransitionMetrics[F[_]: Async](
    startTime: Instant,
    runtimeUI: RuntimeUI,
    origStatus: RuntimeStatus,
    finalStatus: RuntimeStatus,
    cloudService: CloudService
  )(implicit openTelemetry: OpenTelemetryMetrics[F]): F[Unit] =
    for {
      endTime <- Async[F].realTimeInstant
      metricsName = s"monitor/transition/${origStatus}_to_${finalStatus}"
      duration = (endTime.toEpochMilli - startTime.toEpochMilli).millis
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
                                4.5 minutes
      ) // Distribution buckets from 0.5 min to 4.5 min
      _ <- openTelemetry.recordDuration(metricsName, duration, distributionBucket, tags)
    } yield ()

  private def findToolImageInfo(images: Set[RuntimeImage], imageConfig: ImageConfig, custom: Boolean): String = {
    val terraJupyterImage = imageConfig.jupyterImageRegex.r
    val anvilRStudioImage = imageConfig.rstudioImageRegex.r
    val broadDockerhubImageRegex = imageConfig.broadDockerhubImageRegex.r
    images.find(runtimeImage =>
      Set(RuntimeImageType.Jupyter, RuntimeImageType.RStudio) contains runtimeImage.imageType
    ) match {
      case Some(toolImage) =>
        toolImage.imageUrl match {
          case terraJupyterImage(imageType, hash) if !custom        => s"GCR/${imageType}/${hash}"
          case anvilRStudioImage(imageType, hash) if !custom        => s"GCR/${imageType}/${hash}"
          case broadDockerhubImageRegex(imageType, hash) if !custom => s"DockerHub/${imageType}/${hash}"
          case _                                                    => "custom_image"
        }
      case None => "unknown"
    }
  }

  private[monitor] def recordClusterCreationMetrics[F[_]: Async](
    createdDate: Instant,
    images: Set[RuntimeImage],
    imageConfig: ImageConfig,
    cloudService: CloudService,
    custom: Boolean
  )(implicit openTelemetry: OpenTelemetryMetrics[F]): F[Unit] =
    for {
      endTime <- Async[F].realTimeInstant
      toolImageInfo = findToolImageInfo(images, imageConfig, custom)
      metricsName = s"monitor/runtimeCreation"
      duration = (endTime.toEpochMilli - createdDate.toEpochMilli).milliseconds
      tags = Map("cloudService" -> cloudService.asString, "image" -> toolImageInfo)
      _ <- openTelemetry.incrementCounter(metricsName, 1, tags)
      distributionBucket = List(1 minutes,
                                2 minutes,
                                3 minutes,
                                5 minutes,
                                7 minutes,
                                10 minutes,
                                15 minutes,
                                20 minutes,
                                25 minutes,
                                30 minutes
      ) // Distribution buckets from 1 min to 30 min
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
  val loggingContext = Map("traceId" -> traceId.asString, "action" -> action.toString)
}

sealed abstract class MonitorState extends Product with Serializable {
  // Indicates whether the status transition is changed from what the stream has started with.
  // This can happen when a monitor stream started as monitoring for `Starting`, but somehow starting fails, hence,
  // we need to transition runtime to `Stopped` now
  def newTransition: Option[RuntimeStatus]
}
object MonitorState {
  final case object Initial extends MonitorState {
    def newTransition: Option[RuntimeStatus] = None
  }
  final case class Check(runtimeAndRuntimeConfig: RuntimeAndRuntimeConfig,
                         override val newTransition: Option[RuntimeStatus]
  ) extends MonitorState
}

sealed trait MonitorConfig {
  def initialDelay: FiniteDuration
  def pollStatus: PollMonitorConfig
  def monitorStatusTimeouts: Map[RuntimeStatus, FiniteDuration]
  def checkTools: InterruptablePollMonitorConfig
  def runtimeBucketConfig: RuntimeBucketConfig
  def imageConfig: ImageConfig
}

object MonitorConfig {
  final case class GceMonitorConfig(initialDelay: FiniteDuration,
                                    pollStatus: PollMonitorConfig,
                                    monitorStatusTimeouts: Map[RuntimeStatus, FiniteDuration],
                                    checkTools: InterruptablePollMonitorConfig,
                                    runtimeBucketConfig: RuntimeBucketConfig,
                                    imageConfig: ImageConfig
  ) extends MonitorConfig

  final case class DataprocMonitorConfig(initialDelay: FiniteDuration,
                                         pollStatus: PollMonitorConfig,
                                         monitorStatusTimeouts: Map[RuntimeStatus, FiniteDuration],
                                         checkTools: InterruptablePollMonitorConfig,
                                         runtimeBucketConfig: RuntimeBucketConfig,
                                         imageConfig: ImageConfig,
                                         vpcConfig: VPCConfig
  ) extends MonitorConfig
}
