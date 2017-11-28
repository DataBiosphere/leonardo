package org.broadinstitute.dsde.workbench.leonardo.api

import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server.{Directive0, Directive1, Directives}
import org.broadinstitute.dsde.workbench.leonardo.model.{LeoException, UserInfo}
import org.broadinstitute.dsde.workbench.model.WorkbenchEmail

/**
  * Created by rtitle on 10/16/17.
  */

trait UserInfoDirectives {
  def requireUserInfo: Directive1[UserInfo]
}
