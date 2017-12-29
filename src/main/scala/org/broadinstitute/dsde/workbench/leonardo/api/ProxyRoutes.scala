package org.broadinstitute.dsde.workbench.leonardo.api

import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.{AuthorizationFailedRejection, Directive1, Route}
import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.dsde.workbench.leonardo.model.ClusterName
import org.broadinstitute.dsde.workbench.leonardo.service.ProxyService
import org.broadinstitute.dsde.workbench.model.google.GoogleProject
import akka.http.scaladsl.model.headers.{Authorization, HttpCookie}
import org.broadinstitute.dsde.workbench.leonardo.config.ProxyConfig

import scala.concurrent.ExecutionContext

/**
  * Created by rtitle on 8/4/17.
  */
trait ProxyRoutes extends UserInfoDirectives{ self: LazyLogging =>
  val proxyService: ProxyService
  val proxyConfig: ProxyConfig
  implicit val executionContext: ExecutionContext

  protected val tokenCookieName = "FCtoken"

  protected val proxyRoutes: Route =
    path("notebooks" / "login") {
      extractToken(setTokenCookie = true) { token =>
        complete {
          proxyService.getCachedUserInfoFromToken(token).map { userInfo =>
            logger.debug(s"Successfully logged in $userInfo")
            StatusCodes.OK
          }
        }
      }
    } ~
    path("notebooks" / "logout") {
      extractToken(setTokenCookie = false) { token =>
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
        extractToken(setTokenCookie = false) { token => // rejected with AuthorizationFailedRejection if the token is not present
          complete {
            proxyService.getCachedUserInfoFromToken(tokenCookie.value).flatMap { userInfo =>
              // Proxy logic handled by the ProxyService class
              proxyService.proxyLocalize(userInfo, GoogleProject(googleProject), ClusterName(clusterName), request, tokenCookie)
            }
          }
        }
      }
    } ~
    pathPrefix("notebooks" / Segment / Segment) { (googleProject, clusterName) =>
      extractRequest { request =>
        extractToken(setTokenCookie = false) { token => // rejected with AuthorizationFailedRejection if the token is not present
          complete {
            proxyService.getCachedUserInfoFromToken(token).flatMap { userInfo =>
              // Proxy logic handled by the ProxyService class
              proxyService.proxy(userInfo, GoogleProject(googleProject), ClusterName(clusterName), request, tokenCookie)
            }
          }
        }
      }
    }

  /**
    * Extracts the user token from either a cookie or Authorization header.
    * If coming from an Authorization header, tacks on a Set-Cookie header in the response.
    */
  private def extractToken(setTokenCookie: Boolean): Directive1[String] = {
    optionalCookie(tokenCookieName) flatMap {
      // We have a cookie, we're done
      case Some(cookie) => provide(cookie.value)

      // We don't have a cookie, check the Authorization header
      case None => optionalHeaderValueByType[`Authorization`](()) flatMap {

        // We have an Authorization header, extract the token and tack on a Set-Cookie header
        case Some(header) =>
          val token = header.credentials.token()
          if (setTokenCookie) {
            setCookie(buildCookie(token)) tmap { _ => token }
          } else {
            provide(token)
          }

        // Not found in cookie or Authorization header, fail
        case None => reject(AuthorizationFailedRejection)
      }
    }
  }

  private def buildCookie(token: String): HttpCookie = {
    HttpCookie(
      name = tokenCookieName,
      value = token,
      domain = Option(proxyConfig.cookieDomain),
      maxAge = Option(proxyConfig.cookieMaxAge),
      path = Option(proxyConfig.cookiePath)
    )
  }

}
