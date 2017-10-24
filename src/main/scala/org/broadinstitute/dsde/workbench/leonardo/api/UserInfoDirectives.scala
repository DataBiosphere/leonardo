package org.broadinstitute.dsde.workbench.leonardo.api

import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server.{Directive0, Directive1, Directives}
import org.broadinstitute.dsde.workbench.leonardo.model.{LeoException, UserInfo}
import org.broadinstitute.dsde.workbench.model.WorkbenchUserEmail

/**
  * Created by rtitle on 10/16/17.
  */
case class AuthorizationError(email: WorkbenchUserEmail) extends LeoException(s"'$email' is unauthorized", StatusCodes.Unauthorized)

trait UserInfoDirectives {
  def requireUserInfo: Directive1[UserInfo]
  def whitelistConfig: Set[WorkbenchUserEmail]

  def checkWhiteList(userEmail: WorkbenchUserEmail): Directive0 = {
    Directives.mapInnerRoute { r =>
      if (!whitelistConfig.contains(userEmail)) throw AuthorizationError(userEmail)
      else r
    }
  }
}
