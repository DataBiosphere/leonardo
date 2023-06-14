package org.broadinstitute.dsde.workbench.leonardo
package monitor

import cats.effect.IO
import cats.mtl.Ask
import fs2.Stream
import org.broadinstitute.dsde.workbench.model.TraceId

import scala.concurrent.duration.FiniteDuration

class MockRuntimeMonitor extends RuntimeMonitor[IO, CloudService] {
  def process(
    a: CloudService
  )(runtimeId: Long, action: RuntimeStatus, checkToolsInterruptAfter: Option[FiniteDuration])(implicit
    ev: Ask[IO, TraceId]
  ): Stream[IO, Unit] =
    Stream.emit(()).covary[IO]

  def handlePollCheckCompletion(
    a: CloudService
  )(monitorContext: MonitorContext, runtimeAndRuntimeConfig: RuntimeAndRuntimeConfig): IO[Unit] = IO.unit
}

object MockRuntimeMonitor extends MockRuntimeMonitor
