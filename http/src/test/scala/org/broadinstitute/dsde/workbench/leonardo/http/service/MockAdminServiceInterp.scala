package org.broadinstitute.dsde.workbench.leonardo.http.service

import cats.effect.IO
import cats.mtl.Ask
import org.broadinstitute.dsde.workbench.leonardo.AppContext
import org.broadinstitute.dsde.workbench.leonardo.http.UpdateAppsRequest
import org.broadinstitute.dsde.workbench.model.UserInfo

object MockAdminServiceInterp extends AdminService[IO]{

  def updateApps (
    userInfo: UserInfo,
    req: UpdateAppsRequest
  )(implicit as: Ask[IO, AppContext]): IO[Unit] = {
    IO.unit
  }
}
