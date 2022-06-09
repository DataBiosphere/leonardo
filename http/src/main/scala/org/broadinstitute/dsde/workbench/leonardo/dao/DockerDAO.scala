package org.broadinstitute.dsde.workbench.leonardo.dao

import cats.mtl.Ask
import org.broadinstitute.dsde.workbench.leonardo.{ContainerImage, RuntimeImage}
import org.broadinstitute.dsde.workbench.model.TraceId

import java.time.Instant

trait DockerDAO[F[_]] {

  def detectTool(image: ContainerImage, petTokenOpt: Option[String] = None, now: Instant)(implicit
    ev: Ask[F, TraceId]
  ): F[RuntimeImage]

}
