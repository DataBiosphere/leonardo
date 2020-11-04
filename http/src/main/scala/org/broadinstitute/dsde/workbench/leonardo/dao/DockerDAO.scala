package org.broadinstitute.dsde.workbench.leonardo.dao

import cats.mtl.Ask
import org.broadinstitute.dsde.workbench.leonardo.{ContainerImage, RuntimeImageType}
import org.broadinstitute.dsde.workbench.model.TraceId

trait DockerDAO[F[_]] {

  def detectTool(image: ContainerImage, petTokenOpt: Option[String] = None)(
    implicit ev: Ask[F, TraceId]
  ): F[RuntimeImageType]

}
