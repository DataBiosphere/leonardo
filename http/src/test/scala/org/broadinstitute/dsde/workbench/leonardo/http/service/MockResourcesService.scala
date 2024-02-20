package org.broadinstitute.dsde.workbench.leonardo.http.service

import cats.mtl.Ask
import cats.effect.IO
import org.broadinstitute.dsde.workbench.leonardo.{AppContext, CloudContext}
import org.broadinstitute.dsde.workbench.model.UserInfo

object MockResourcesService extends ResourcesService[IO] {

  def deleteAllResourcesInCloud(userInfo: UserInfo, cloudContext: CloudContext.Gcp, deleteDisk: Boolean)(implicit
    as: Ask[IO, AppContext]
  ): IO[Unit] = IO.unit

  def deleteAllResourcesRecords(userInfo: UserInfo, cloudContext: CloudContext.Gcp)(implicit
    as: Ask[IO, AppContext]
  ): IO[Unit] = IO.unit
}
