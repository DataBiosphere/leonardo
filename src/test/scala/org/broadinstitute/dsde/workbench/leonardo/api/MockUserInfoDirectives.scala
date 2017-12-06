package org.broadinstitute.dsde.workbench.leonardo.api

import akka.http.scaladsl.server.Directive1
import akka.http.scaladsl.server.Directives.provide
import org.broadinstitute.dsde.workbench.model.UserInfo

/**
  * Created by rtitle on 10/16/17.
  */
trait MockUserInfoDirectives extends UserInfoDirectives {
  val userInfo: UserInfo

  override def requireUserInfo: Directive1[UserInfo] = provide(userInfo)
}
