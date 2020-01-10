package org.broadinstitute.dsde.workbench.leonardo.dao

import cats.mtl.ApplicativeAsk
import org.broadinstitute.dsde.workbench.leonardo.model.{ClusterImageType, ContainerImage}
import org.broadinstitute.dsde.workbench.model.{TraceId, UserInfo}

trait DockerDAO[F[_]] {

  def detectTool(image: ContainerImage, userInfo: UserInfo)(
    implicit ev: ApplicativeAsk[F, TraceId]
  ): F[Option[ClusterImageType]]

}
