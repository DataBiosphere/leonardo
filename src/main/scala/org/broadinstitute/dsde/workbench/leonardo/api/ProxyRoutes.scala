package org.broadinstitute.dsde.workbench.leonardo.api

import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.{AuthorizationFailedRejection, Directive0, Directive1, Route}
import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.dsde.workbench.leonardo.model.ClusterName
import org.broadinstitute.dsde.workbench.leonardo.service.ProxyService
import org.broadinstitute.dsde.workbench.model.google.GoogleProject
import akka.http.scaladsl.model.headers._
import org.broadinstitute.dsde.workbench.leonardo.model.NotebookClusterActions.ConnectToCluster
import org.broadinstitute.dsde.workbench.model.UserInfo

import scala.concurrent.ExecutionContext

trait ProxyRoutes extends UserInfoDirectives { self: LazyLogging =>
  val proxyService: ProxyService
  implicit val executionContext: ExecutionContext

  protected val tokenCookieName = "FCtoken"

  protected val proxyRoutes: Route =
    pathPrefix("notebooks") {
      pathPrefix(Segment / Segment) { (googleProjectParam, clusterNameParam) =>
        val googleProject = GoogleProject(googleProjectParam)
        val clusterName = ClusterName(clusterNameParam)

        (extractRequest & extractUserInfo) { (request, userInfo) =>
          path("setCookie") {
            get {
              // Check the user for ConnectToCluster privileges and set a cookie in the response
              onSuccess(proxyService.authCheck(userInfo, googleProject, clusterName, ConnectToCluster)) {
                setTokenCookie(userInfo) {
                  addAccessControlHeaders {
                    complete {
                      logger.debug(s"Successfully set cookie for user $userInfo")
                      StatusCodes.OK
                    }
                  }
                }
              }
            }
          } ~
            // Proxy logic handled by the ProxyService class
            // Note ProxyService calls the LeoAuthProvider internally
            path("api" / "localize") { // route for custom Jupyter server extension
              complete {
                proxyService.proxyLocalize(userInfo, googleProject, clusterName, request)
              }
            } ~
            complete {
              proxyService.proxyNotebook(userInfo, googleProject, clusterName, request)
            }
        }
      } ~
        // No need to lookup the user or consult the auth provider for this endpoint
        path("invalidateToken") {
          get {
            extractToken { token =>
              complete {
                proxyService.invalidateAccessToken(token).map { _ =>
                  logger.debug(s"Invalidated access token $token")
                  StatusCodes.OK
                }
              }
            }
          }
        }
    }

  /**
    * Extracts the user token from either a cookie or Authorization header.
    */
  private def extractToken: Directive1[String] = {
    optionalCookie(tokenCookieName) flatMap {
      // We have a cookie, we're done
      case Some(cookie) => provide(cookie.value)

      // We don't have a cookie; check the Authorization header
      case None => optionalHeaderValueByType[`Authorization`](()) flatMap {

        // We have an Authorization header, extract the token
        case Some(header) => provide(header.credentials.token)

        // Not found in cookie or Authorization header, fail
        case None => reject(AuthorizationFailedRejection)
      }
    }
  }

  /**
    * Extracts the user token from the request, and looks up the cached UserInfo.
    */
  private def extractUserInfo: Directive1[UserInfo] = {
    extractToken.flatMap { token =>
      onSuccess(proxyService.getCachedUserInfoFromToken(token))
    }
  }

  /**
    * Sets a token cookie in the HTTP response.
    */
  private def setTokenCookie(userInfo: UserInfo): Directive0 = {
    setCookie(buildCookie(userInfo))
  }

  private def addAccessControlHeaders: Directive0 = {
    optionalHeaderValueByType[`Origin`](()) flatMap {
      case Some(origin) => respondWithHeaders(
        `Access-Control-Allow-Origin`(origin.value),
        `Access-Control-Allow-Credentials`(true))
      case None =>
        reject(AuthorizationFailedRejection)
    }

  }
  private def buildCookie(userInfo: UserInfo): HttpCookie = {
    HttpCookie(
      name = tokenCookieName,
      value = userInfo.accessToken.token,
      secure = true,  // cookie is only sent for SSL requests
      domain = None,  // Do not specify domain, making it default to Leo's domain
      maxAge = Option(userInfo.tokenExpiresIn / 1000),  // coookie expiry is tied to the token expiry
      path = Some("/")  // needed so it works for AJAX requests
    )
  }

}
