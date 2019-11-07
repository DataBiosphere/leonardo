package org.broadinstitute.dsde.workbench.leonardo.dao

import cats.effect.IO
import cats.mtl.ApplicativeAsk
import org.broadinstitute.dsde.workbench.leonardo.model.ClusterTool.Jupyter
import org.broadinstitute.dsde.workbench.leonardo.model.{ClusterTool, ContainerImage}
import org.broadinstitute.dsde.workbench.model.TraceId

class MockDockerDAO(tool: ClusterTool = Jupyter) extends DockerDAO[IO] {
  override def detectTool(image: ContainerImage)(implicit ev: ApplicativeAsk[IO, TraceId]): IO[Option[ClusterTool]] =
    IO.pure(Some(tool))
}
