package org.broadinstitute.dsde.workbench.leonardo.api

import akka.event.{Logging, LoggingAdapter}
import akka.http.scaladsl.model._
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.{Directive0, Directive1, Route}
import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.dsde.workbench.leonardo.model.google.ClusterName
import org.broadinstitute.dsde.workbench.leonardo.service.{AuthenticationError, AuthorizationError, ProxyService}
import org.broadinstitute.dsde.workbench.model.google.GoogleProject
import akka.http.scaladsl.model.headers._
import akka.http.scaladsl.server.RouteResult.Complete
import akka.http.scaladsl.server.directives.{DebuggingDirectives, LogEntry, LoggingMagnet}
import org.broadinstitute.dsde.workbench.leonardo.model.NotebookClusterActions.ConnectToCluster
import org.broadinstitute.dsde.workbench.leonardo.util.CookieHelper
import org.broadinstitute.dsde.workbench.model.UserInfo

import scala.concurrent.ExecutionContext

trait ProxyRoutes extends UserInfoDirectives with CorsSupport with CookieHelper { self: LazyLogging =>
  val proxyService: ProxyService
  implicit val executionContext: ExecutionContext

  protected val proxyRoutes: Route =
    pathPrefix("notebooks") {

      corsHandler {

        pathPrefix(Segment / Segment) { (googleProjectParam, clusterNameParam) =>
          val googleProject = GoogleProject(googleProjectParam)
          val clusterName = ClusterName(clusterNameParam)

          path("setCookie") {
            extractUserInfo { userInfo =>
              get {
                // Check the user for ConnectToCluster privileges and set a cookie in the response
                onSuccess(proxyService.authCheck(userInfo, googleProject, clusterName, ConnectToCluster)) {
                  setTokenCookie(userInfo, tokenCookieName) {
                    complete {
                      logger.debug(s"Successfully set cookie for user $userInfo")
                      StatusCodes.NoContent
                    }
                  }
                }
              }
            }
          } ~
            (extractRequest & extractUserInfo) { (request, userInfo) =>
              (logRequestResultForMetrics(userInfo)) {
                // Proxy logic handled by the ProxyService class
                // Note ProxyService calls the LeoAuthProvider internally
                path("api" / "localize") { // route for custom Jupyter server extension
                  complete {
                    val proxyFuture = proxyService.proxyLocalize(userInfo, googleProject, clusterName, request)
                    // we are discarding the request entity here. we have noticed that PUT requests caused by
                    // saving a notebook when a cluster is stopped correlate perfectly with CPU spikes.
                    // in that scenario, the requests appear to pile up, causing apache to hog CPU.
                    proxyFuture.failed.foreach { _ =>
                      request.entity.discardBytes().future
                    }
                    proxyFuture
                  }
                } ~
                  complete {
                    val proxyFuture = proxyService.proxyNotebook(userInfo, googleProject, clusterName, request)
                    // we are discarding the request entity here. we have noticed that PUT requests caused by
                    // saving a notebook when a cluster is stopped correlate perfectly with CPU spikes.
                    // in that scenario, the requests appear to pile up, causing apache to hog CPU.
                    proxyFuture.failed.foreach { _ =>
                      request.entity.discardBytes().future
                    }
                    proxyFuture
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
    }

  /**
    * Extracts the user token from either a cookie or Authorization header.
    */
  private def extractToken: Directive1[String] = {
    optionalHeaderValueByType[`Authorization`](()) flatMap {

      // We have an Authorization header, extract the token
      // Note the Authorization header overrides the cookie
      case Some(header) => provide(header.credentials.token)

      // We don't have an Authorization header; check the cookie
      case None => optionalCookie(tokenCookieName) flatMap {

        // We have a cookie, extract the token
        case Some(cookie) => provide(cookie.value)

        // Not found in cookie or Authorization header, fail
        case None => failWith(AuthenticationError())
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

  // basis for logRequestResult lifted from http://stackoverflow.com/questions/32475471/how-does-one-log-akka-http-client-requests
  private def logRequestResultForMetrics(userInfo: UserInfo): Directive0 = {
    def myMetricsLogger(logger: LoggingAdapter)(req: HttpRequest)(res: Any): Unit = {
      val headers = req.headers
      val headerMap: Map[String, String] = headers.map { header =>
        (header.name(), header.value())
      }.toMap

      val entry = res match {
        case Complete(resp) =>
          LogEntry(s"${headerMap.getOrElse("X-Forwarded-For", "0.0.0.0")} ${userInfo.userEmail} " +
            s"${userInfo.userId} [${DateTime.now.toIsoDateTimeString()}] " +
            s""""${req.method.value} ${req.uri} ${req.protocol.value}" """ +
            s"""${resp.status.intValue} ${resp.entity.contentLengthOption.getOrElse("-")} ${headerMap.getOrElse("Origin", "-")} """ +
            s"${headerMap.getOrElse("User-Agent", "unknown")}", Logging.InfoLevel)
        case _ => LogEntry(s"No response - request not complete")
      }
      entry.logTo(logger)
    }
    DebuggingDirectives.logRequestResult(LoggingMagnet(myMetricsLogger))
  }
}
