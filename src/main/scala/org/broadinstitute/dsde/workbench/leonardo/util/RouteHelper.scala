package org.broadinstitute.dsde.workbench.leonardo.util

import akka.http.scaladsl.model.headers.HttpCookie
import akka.http.scaladsl.server.Directive0
import akka.http.scaladsl.server.Directives.setCookie
import org.broadinstitute.dsde.workbench.model.UserInfo

trait RouteHelper {
  /**
    * Sets a token cookie in the HTTP response.
    */
  def setTokenCookie(userInfo: UserInfo, cookieName: String): Directive0 = {
    setCookie(buildCookie(userInfo, cookieName))
  }

  def buildCookie(userInfo: UserInfo, cookieName: String): HttpCookie = {
    HttpCookie(
      name = cookieName,
      value = userInfo.accessToken.token,
      secure = true,  // cookie is only sent for SSL requests
      domain = None,  // Do not specify domain, making it default to Leo's domain
      maxAge = Option(userInfo.tokenExpiresIn),  // coookie expiry is tied to the token expiry
      path = Some("/")  // needed so it works for AJAX requests
    )
  }
}
