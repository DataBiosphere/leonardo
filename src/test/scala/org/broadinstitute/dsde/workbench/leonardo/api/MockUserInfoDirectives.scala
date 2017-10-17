package org.broadinstitute.dsde.workbench.leonardo.api

import java.time.Duration

import akka.http.scaladsl.model.headers.OAuth2BearerToken
import akka.http.scaladsl.server.Directive1
import akka.http.scaladsl.server.Directives.provide
import org.broadinstitute.dsde.workbench.leonardo.model.UserInfo
import org.broadinstitute.dsde.workbench.model.{WorkbenchUserEmail, WorkbenchUserId}

/**
  * Created by rtitle on 10/16/17.
  */
trait MockUserInfoDirectives extends UserInfoDirectives {
  val userInfo: UserInfo =
    UserInfo(OAuth2BearerToken(""),
      WorkbenchUserId(""),
      WorkbenchUserEmail("test@test.com"),
      10)

  override def requireUserInfo: Directive1[UserInfo] = provide(userInfo)
}
