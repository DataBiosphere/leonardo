package org.broadinstitute.dsde.workbench.leonardo.api

import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server.{Directive0, Directive1, Directives}
import org.broadinstitute.dsde.workbench.leonardo.model.{LeoException, UserInfo}
import org.broadinstitute.dsde.workbench.model.WorkbenchEmail

/**
  * Created by rtitle on 10/16/17.
  */
case class AuthorizationError(email: WorkbenchEmail) extends LeoException(s"'$email' is unauthorized", StatusCodes.Unauthorized)

trait UserInfoDirectives {
  def requireUserInfo: Directive1[UserInfo]
  def whitelistConfig: Set[WorkbenchEmail]

  def checkWhiteList(userEmail: WorkbenchEmail): Directive0 = {
    Directives.mapInnerRoute { r =>
      if (!whitelistConfig.contains(userEmail)) throw AuthorizationError(userEmail)
      else r
    }
  }
}
