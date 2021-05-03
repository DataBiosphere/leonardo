package org.broadinstitute.dsde.workbench.leonardo
package dao

import cats.effect.IO
import cats.mtl.Ask
import org.broadinstitute.dsde.workbench.leonardo.RuntimeImageType.Jupyter
import org.broadinstitute.dsde.workbench.model.TraceId

import java.time.Instant

class MockDockerDAO(tool: RuntimeImageType = Jupyter) extends DockerDAO[IO] {
  override def detectTool(
    image: ContainerImage,
    petTokenOpt: Option[String],
    now: Instant
  )(implicit ev: Ask[IO, TraceId]): IO[RuntimeImage] =
    IO.pure(RuntimeImage(tool, "image-url", None, now))
}
