package org.broadinstitute.dsde.workbench.leonardo.dao

import cats.effect.IO
import cats.mtl.ApplicativeAsk
import org.broadinstitute.dsde.workbench.leonardo.model.ClusterImageType.Jupyter
import org.broadinstitute.dsde.workbench.leonardo.model.{ClusterImageType, ContainerImage}
import org.broadinstitute.dsde.workbench.model.TraceId

class MockDockerDAO(tool: ClusterImageType = Jupyter) extends DockerDAO[IO] {
  override def detectTool(
    image: ContainerImage,
    petTokenOpt: Option[String]
  )(implicit ev: ApplicativeAsk[IO, TraceId]): IO[Option[ClusterImageType]] =
    IO.pure(Some(tool))
}
