package org.broadinstitute.dsde.workbench.leonardo.api

import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.{AuthorizationFailedRejection, Directive0, Directive1, Route}
import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.dsde.workbench.leonardo.model.ClusterName
import org.broadinstitute.dsde.workbench.leonardo.service.ProxyService
import org.broadinstitute.dsde.workbench.model.google.GoogleProject
import akka.http.scaladsl.model.headers.{Authorization, HttpCookie}
import org.broadinstitute.dsde.workbench.model.UserInfo

import scala.concurrent.ExecutionContext

trait ProxyRoutes extends UserInfoDirectives { self: LazyLogging =>
  val proxyService: ProxyService
  implicit val executionContext: ExecutionContext

  protected val tokenCookieName = "FCtoken"

  protected val proxyRoutes: Route =
    path("notebooks" / "login") {
      extractUserInfo { userInfo => // rejected with AuthorizationFailedRejection if the token is not present
        setTokenCookie(userInfo) {  // set the token cookie in the response
          complete {
            logger.debug(s"Successfully logged in user $userInfo")
            StatusCodes.OK
          }
        }
      }
    } ~
    path("notebooks" / "logout") {
      extractToken { token =>
        complete {
          proxyService.invalidateAccessToken(token).map { _ =>
            logger.debug(s"Invalidated access token $token")
            StatusCodes.OK
          }
        }
      }
    } ~
    path("notebooks" / Segment / Segment / "api" / "localize") { (googleProject, clusterName) =>
      extractRequest { request =>
        extractUserInfo { userInfo => // rejected with AuthorizationFailedRejection if the token is not present
          complete {
            // Proxy logic handled by the ProxyService class
            proxyService.proxyLocalize(userInfo, GoogleProject(googleProject), ClusterName(clusterName), request)
          }
        }
      }
    } ~
    pathPrefix("notebooks" / Segment / Segment) { (googleProject, clusterName) =>
      extractRequest { request =>
        extractUserInfo { userInfo => // rejected with AuthorizationFailedRejection if the token is not present
          complete {
            // Proxy logic handled by the ProxyService class
            proxyService.proxyNotebook(userInfo, GoogleProject(googleProject), ClusterName(clusterName), request)
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
