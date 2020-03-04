package org.broadinstitute.dsde.workbench.leonardo
package dao

import cats.effect.IO
import cats.mtl.ApplicativeAsk
import org.broadinstitute.dsde.workbench.leonardo.RuntimeImageType.Jupyter
import org.broadinstitute.dsde.workbench.model.TraceId

class MockDockerDAO(tool: RuntimeImageType = Jupyter) extends DockerDAO[IO] {
  override def detectTool(
    image: ContainerImage,
    petTokenOpt: Option[String]
  )(implicit ev: ApplicativeAsk[IO, TraceId]): IO[Option[RuntimeImageType]] =
    IO.pure(Some(tool))
}
