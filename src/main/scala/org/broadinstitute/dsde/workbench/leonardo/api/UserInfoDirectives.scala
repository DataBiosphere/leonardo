package org.broadinstitute.dsde.workbench.leonardo.api

import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server.{Directive0, Directive1, Directives, Route}
import org.broadinstitute.dsde.workbench.leonardo.model.{LeoException, UserInfo}
import org.broadinstitute.dsde.workbench.model.{WorkbenchExceptionWithErrorReport, WorkbenchUserEmail}

/**
  * Created by rtitle on 10/16/17.
  */
case class AuthorizationError(email: WorkbenchUserEmail) extends LeoException(s"'$email' is unauthorized", StatusCodes.Unauthorized)

trait UserInfoDirectives {
  def requireUserInfo: Directive1[UserInfo]
  def whiteListConfig: Set[WorkbenchUserEmail]

  def checkWhiteList(userEmail: WorkbenchUserEmail): Directive0 = {
    Directives.mapInnerRoute { r =>
      if (!whiteListConfig.contains(userEmail)) throw AuthorizationError(userEmail)
      else r
    }
  }
}
