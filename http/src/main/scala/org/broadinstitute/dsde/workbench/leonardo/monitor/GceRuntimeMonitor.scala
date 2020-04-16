package org.broadinstitute.dsde.workbench.leonardo.monitor

import java.time.Instant

import cats.mtl.ApplicativeAsk
import fs2.Stream
import org.broadinstitute.dsde.workbench.google2.ZoneName
import org.broadinstitute.dsde.workbench.leonardo._
import org.broadinstitute.dsde.workbench.leonardo.config.{ImageConfig, RuntimeBucketConfig}
import org.broadinstitute.dsde.workbench.model.TraceId
import org.broadinstitute.dsde.workbench.model.google.GoogleProject

import scala.concurrent.duration._

/**
 * Monitor for runtime status transition: Starting, Creating, Deleting, Stopping
 * It doesn't trigger any of the action but only responsible for monitoring the progress and make necessary cleanup when the transition is done
 */
trait GceRuntimeMonitor[F[_]] {
  def process(runtimeId: Long)(implicit ev: ApplicativeAsk[F, TraceId]): Stream[F, Unit]

  // Function used for transitions that we can get an Operation
  def pollCheck(googleProject: GoogleProject,
                runtimeAndRuntimeConfig: RuntimeAndRuntimeConfig,
                operation: com.google.cloud.compute.v1.Operation,
                action: RuntimeStatus)(implicit ev: ApplicativeAsk[F, TraceId]): F[Unit]
}

object GceRuntimeMonitor {
  type CheckResult = (Unit, Option[GceMonitorState])
}

sealed abstract class GceMonitorState extends Product with Serializable
object GceMonitorState {
  final case object Initial extends GceMonitorState
  final case class Check(runtimeAndRuntimeConfig: RuntimeAndRuntimeConfig) extends GceMonitorState
  final case class CheckTools(ip: IP,
                              runtimeAndRuntimeConfig: RuntimeAndRuntimeConfig,
                              toolsToCheck: List[RuntimeImageType])
      extends GceMonitorState
}

final case class MonitorContext(start: Instant, runtimeId: Long, traceId: TraceId) {
  override def toString: String = s"runtimeId(${runtimeId}, traceId(${traceId.asString}))"
}

final case class GceMonitorConfig(initialDelay: FiniteDuration,
                                  pollingInterval: FiniteDuration,
                                  pollCheckMaxAttempts: Int,
                                  checkToolsDelay: FiniteDuration,
                                  runtimeBucketConfig: RuntimeBucketConfig,
                                  monitorStatusTimeouts: Map[RuntimeStatus, FiniteDuration],
                                  gceZoneName: ZoneName,
                                  imageConfig: ImageConfig)
