package org.broadinstitute.dsde.workbench.leonardo
package monitor

import cats.effect.IO
import cats.mtl.Ask
import fs2.Stream
import org.broadinstitute.dsde.workbench.model.TraceId
import org.broadinstitute.dsde.workbench.model.google.GoogleProject

class MockRuntimeMonitor extends RuntimeMonitor[IO, CloudService] {
  def process(a: CloudService)(runtimeId: Long,
                               action: RuntimeStatus)(implicit ev: Ask[IO, TraceId]): Stream[IO, Unit] =
    Stream.emit(()).covary[IO]

  // Function used for transitions that we can get an Operation
  def pollCheck(a: CloudService)(googleProject: GoogleProject,
                                 runtimeAndRuntimeConfig: RuntimeAndRuntimeConfig,
                                 operation: com.google.cloud.compute.v1.Operation,
                                 action: RuntimeStatus)(implicit ev: Ask[IO, TraceId]): IO[Unit] = IO.unit
}

object MockRuntimeMonitor extends MockRuntimeMonitor
