package org.broadinstitute.dsde.workbench.leonardo.http
package api

import akka.http.scaladsl.model.headers.RawHeader
import akka.http.scaladsl.server.Directive0
import akka.http.scaladsl.server.directives.RespondWithDirectives.respondWithHeaders
import java.net.URL

import org.broadinstitute.dsde.workbench.leonardo.config.Config
import org.broadinstitute.dsde.workbench.model.UserInfo

object CookieSupport {
  val tokenCookieName = "LeoToken"
  val partitionedTokenCookieName = "TerraToken"
  val partitionedProxyTokenCookieName = "LeoProxyToken"
  val proxyUrl = new URL(Config.proxyConfig.proxyUrlBase)
  val isPartitioned = Config.proxyConfig.isProxyCookiePartitioned

  /**
   * Sets a token cookie in the HTTP response.
   */
  def setTokenCookie(userInfo: UserInfo): Directive0 =
    if (isPartitioned)
      respondWithHeaders(buildRawCookie(userInfo), buildRawTerraCookie(userInfo), buildRawProxyCookie(userInfo))
    else
      respondWithHeaders(buildRawCookie(userInfo))

  /**
   * Unsets a token cookie in the HTTP response.
   */
  def unsetTokenCookie(): Directive0 =
    if (isPartitioned)
      respondWithHeaders(buildRawUnsetCookie(), buildRawUnsetTerraCookie(), buildRawUnsetProxyCookie())
    else
      respondWithHeaders(buildRawUnsetCookie())

  private def buildRawCookie(userInfo: UserInfo) =
    RawHeader(
      name = "Set-Cookie",
      value =
        s"$tokenCookieName=${userInfo.accessToken.token}; Max-Age=${userInfo.tokenExpiresIn.toString}; Path=/; Secure; SameSite=None; HttpOnly"
    )

  private def buildRawUnsetCookie(): RawHeader =
    RawHeader(
      name = "Set-Cookie",
      value = s"$tokenCookieName=unset; Expires=Thu, 01 Jan 1970 00:00:00 GMT; Path=/; Secure; SameSite=None; HttpOnly"
    )

  private def buildRawTerraCookie(userInfo: UserInfo) =
    RawHeader(
      name = "Set-Cookie",
      value =
        s"$partitionedTokenCookieName=${userInfo.accessToken.token}; Max-Age=${userInfo.tokenExpiresIn.toString}; Path=/; Secure; SameSite=None; HttpOnly; Partitioned"
    )

  private def buildRawUnsetTerraCookie(): RawHeader =
    RawHeader(
      name = "Set-Cookie",
      value =
        s"$partitionedTokenCookieName=unset; Expires=Thu, 01 Jan 1970 00:00:00 GMT; Path=/; Secure; SameSite=None; HttpOnly; Partitioned"
    )

  private def buildRawProxyCookie(userInfo: UserInfo) =
    RawHeader(
      name = "Set-Cookie",
      value =
        s"$partitionedProxyTokenCookieName=${userInfo.accessToken.token}; Domain=${proxyUrl.getHost}; Max-Age=${userInfo.tokenExpiresIn.toString}; Path=/; Secure; SameSite=None; HttpOnly; Partitioned"
    )

  private def buildRawUnsetProxyCookie(): RawHeader =
    RawHeader(
      name = "Set-Cookie",
      value =
        s"$partitionedProxyTokenCookieName=unset; Domain=${proxyUrl.getHost}; Expires=Thu, 01 Jan 1970 00:00:00 GMT; Path=/; Secure; SameSite=None; HttpOnly; Partitioned"
    )
}
