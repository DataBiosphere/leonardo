package org.broadinstitute.dsde.workbench.leonardo.api

import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server.{Directive0, Directive1, Directives}
import org.broadinstitute.dsde.workbench.model.UserInfo

/**
  * Created by rtitle on 10/16/17.
  */

trait UserInfoDirectives {
  def requireUserInfo: Directive1[UserInfo]
}
