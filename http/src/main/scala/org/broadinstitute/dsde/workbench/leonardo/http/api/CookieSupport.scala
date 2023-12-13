package org.broadinstitute.dsde.workbench.leonardo.http
package api

import akka.http.scaladsl.model.headers.{HttpCookie, RawHeader}
import akka.http.scaladsl.server.Directive0
import akka.http.scaladsl.server.directives.RespondWithDirectives.respondWithHeaders
import org.broadinstitute.dsde.workbench.model.UserInfo

object CookieSupport {
  val tokenCookieName = "LeoToken"

  // TODO: in the below 2 methods we set a cookie by building a RawHeader because the cookie
  // SameSite attribute is not natively supported in akka-http. Support is tentatively
  // slated for version 10.2.0, after which we can switch back to using HttpCookie.
  // See https://github.com/akka/akka-http/issues/1354

  /**
   * Sets a token cookie in the HTTP response.
   */
  def setTokenCookie(userInfo: UserInfo): Directive0 =
    // setCookie(buildCookie(userInfo, cookieName))
    respondWithHeaders(buildRawCookie(userInfo))

  /**
   * Unsets a token cookie in the HTTP response.
   */
  def unsetTokenCookie(): Directive0 =
    respondWithHeaders(buildRawUnsetCookie())

  private def buildCookie(userInfo: UserInfo, cookieName: String): HttpCookie =
    HttpCookie(
      name = cookieName,
      value = userInfo.accessToken.token,
      secure = true, // cookie is only sent for SSL requests
      domain = None, // Do not specify domain, making it default to Leo's domain
      maxAge = Option(userInfo.tokenExpiresIn), // cookie expiry is tied to the token expiry
      path = Some("/") // needed so it works for AJAX requests
    )

  private def buildRawCookie(userInfo: UserInfo) =
    RawHeader(
      name = "Set-Cookie",
      value =
        s"$tokenCookieName=${userInfo.accessToken.token}; Max-Age=${userInfo.tokenExpiresIn.toString}; Path=/; Secure; SameSite=None; HttpOnly"
    )

  private def buildRawUnsetCookie(): RawHeader =
    RawHeader(
      name = "Set-Cookie",
      value = s"$tokenCookieName=unset; expires=Thu, 01 Jan 1970 00:00:00 GMT; Path=/; Secure; SameSite=None; HttpOnly"
    )

}
