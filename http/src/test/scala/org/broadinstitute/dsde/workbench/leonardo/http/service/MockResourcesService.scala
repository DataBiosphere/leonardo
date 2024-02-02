package org.broadinstitute.dsde.workbench.leonardo.http.service

import cats.mtl.Ask
import cats.effect.IO
import org.broadinstitute.dsde.workbench.leonardo.AppContext
import org.broadinstitute.dsde.workbench.model.google.GoogleProject
import org.broadinstitute.dsde.workbench.model.UserInfo

object MockResourcesService extends ResourcesService[IO] {

  def deleteAllResources(userInfo: UserInfo, googleProject: GoogleProject, deleteInCloud: Boolean, deleteDisk: Boolean)(
    implicit as: Ask[IO, AppContext]
  ): IO[Unit] = IO.unit
}
