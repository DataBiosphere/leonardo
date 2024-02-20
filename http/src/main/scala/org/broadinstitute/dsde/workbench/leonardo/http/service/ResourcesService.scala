package org.broadinstitute.dsde.workbench.leonardo.http.service

import cats.mtl.Ask
import org.broadinstitute.dsde.workbench.leonardo.{AppContext, CloudContext}
import org.broadinstitute.dsde.workbench.model.UserInfo

trait ResourcesService[F[_]] {

  def deleteAllResourcesInCloud(userInfo: UserInfo, cloudContext: CloudContext.Gcp, deleteDisk: Boolean)(implicit
    as: Ask[F, AppContext]
  ): F[Unit]

  def deleteAllResourcesRecords(userInfo: UserInfo, cloudContext: CloudContext.Gcp)(implicit
    as: Ask[F, AppContext]
  ): F[Unit]
}
