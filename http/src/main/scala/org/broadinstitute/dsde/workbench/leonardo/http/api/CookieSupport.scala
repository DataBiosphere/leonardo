package org.broadinstitute.dsde.workbench.leonardo.http
package api

import akka.http.scaladsl.model.headers.RawHeader
import akka.http.scaladsl.server.Directive0
import akka.http.scaladsl.server.directives.RespondWithDirectives.respondWithHeaders
import org.broadinstitute.dsde.workbench.model.UserInfo

object CookieSupport {
  val tokenCookieName = "LeoToken"
  val proxyTokenCookieName = "LeoProxyToken"

  /**
   * Sets a token cookie in the HTTP response.
   */
  def setTokenCookie(userInfo: UserInfo): Directive0 =
    respondWithHeaders(buildRawCookie(userInfo), buildRawProxyCookie(userInfo))

  /**
   * Unsets a token cookie in the HTTP response.
   */
  def unsetTokenCookie(): Directive0 =
    respondWithHeaders(buildRawUnsetCookie(), buildRawUnsetProxyCookie())

  private def buildRawCookie(userInfo: UserInfo) =
    RawHeader(
      name = "Set-Cookie",
      value =
        s"$tokenCookieName=${userInfo.accessToken.token}; Max-Age=${userInfo.tokenExpiresIn.toString}; Path=/; Secure; SameSite=None; HttpOnly; Partitioned"
    )

  private def buildRawUnsetCookie(): RawHeader =
    RawHeader(
      name = "Set-Cookie",
      value =
        s"$tokenCookieName=unset; Expires=Thu, 01 Jan 1970 00:00:00 GMT; Path=/; Secure; SameSite=None; HttpOnly; Partitioned"
    )

  private def buildRawProxyCookie(userInfo: UserInfo) =
    RawHeader(
      name = "Set-Cookie",
      value =
        s"$proxyTokenCookieName=${userInfo.accessToken.token}; Domain=leonardo.dsde-dev.broadinstitute.org; Max-Age=${userInfo.tokenExpiresIn.toString}; Path=/; Secure; SameSite=None; HttpOnly; Partitioned"
    )

  private def buildRawUnsetProxyCookie(): RawHeader =
    RawHeader(
      name = "Set-Cookie",
      value =
        s"$proxyTokenCookieName=unset; Domain=leonardo.dsde-dev.broadinstitute.org; Expires=Thu, 01 Jan 1970 00:00:00 GMT; Path=/; Secure; SameSite=None; HttpOnly; Partitioned"
    )
}
