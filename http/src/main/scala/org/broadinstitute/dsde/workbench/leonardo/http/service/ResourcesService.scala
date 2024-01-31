package org.broadinstitute.dsde.workbench.leonardo.http.service

import cats.mtl.Ask
import org.broadinstitute.dsde.workbench.leonardo.AppContext
import org.broadinstitute.dsde.workbench.model.google.GoogleProject
import org.broadinstitute.dsde.workbench.model.UserInfo

trait ResourcesService[F[_]] {
  def deleteAllResources(userInfo: UserInfo, googleProject: GoogleProject, deleteInCloud: Boolean, deleteDisk: Boolean)(
    implicit as: Ask[F, AppContext]
  ): F[Unit]
}
