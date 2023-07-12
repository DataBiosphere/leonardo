package org.broadinstitute.dsde.workbench.leonardo.http.service

import cats.mtl.Ask
import org.broadinstitute.dsde.workbench.leonardo.AppContext
import org.broadinstitute.dsde.workbench.leonardo.http.UpdateAppsRequest
import org.broadinstitute.dsde.workbench.model.UserInfo

trait AdminService[F[_]] {

  def updateApps (
    userInfo: UserInfo,
    req: UpdateAppsRequest
  )(implicit as: Ask[F, AppContext]): F[Unit]
  
}
