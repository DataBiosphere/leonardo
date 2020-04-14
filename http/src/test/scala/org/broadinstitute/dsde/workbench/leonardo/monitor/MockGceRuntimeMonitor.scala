package org.broadinstitute.dsde.workbench.leonardo
package monitor

import cats.effect.IO
import cats.mtl.ApplicativeAsk
import fs2.Stream
import org.broadinstitute.dsde.workbench.model.TraceId
import org.broadinstitute.dsde.workbench.model.google.GoogleProject

object MockGceRuntimeMonitor extends GceRuntimeMonitor[IO] {
  def process(runtimeId: Long)(implicit ev: ApplicativeAsk[IO, TraceId]): Stream[IO, Unit] = Stream.emit(()).covary[IO]

  // Function used for transitions that we can get an Operation
  def pollCheck(googleProject: GoogleProject,
                runtimeAndRuntimeConfig: RuntimeAndRuntimeConfig,
                operation: com.google.cloud.compute.v1.Operation,
                action: RuntimeStatus)(implicit ev: ApplicativeAsk[IO, TraceId]): IO[Unit] = IO.unit
}
