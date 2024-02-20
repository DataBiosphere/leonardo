package org.broadinstitute.dsde.workbench.leonardo.http.service

import cats.mtl.Ask
import cats.effect.IO
import org.broadinstitute.dsde.workbench.leonardo.AppContext
import org.broadinstitute.dsde.workbench.model.google.GoogleProject
import org.broadinstitute.dsde.workbench.model.UserInfo

object MockResourcesService extends ResourcesService[IO] {

  def deleteAllResourcesInCloud(userInfo: UserInfo, googleProject: GoogleProject, deleteDisk: Boolean)(implicit
    as: Ask[IO, AppContext]
  ): IO[Unit] = IO.unit

  def deleteAllResourcesRecords(userInfo: UserInfo, googleProject: GoogleProject)(implicit
    as: Ask[IO, AppContext]
  ): IO[Unit] = IO.unit
}
