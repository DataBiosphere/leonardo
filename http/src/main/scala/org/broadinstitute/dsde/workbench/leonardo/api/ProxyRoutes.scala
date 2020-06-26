package org.broadinstitute.dsde.workbench.leonardo
package http
package api

import java.util.UUID

import akka.event.{Logging, LoggingAdapter}
import akka.http.scaladsl.model._
import akka.http.scaladsl.server.Directives.{path, _}
import akka.http.scaladsl.server.{Directive0, Directive1, Route}
import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.dsde.workbench.leonardo.http.service.{KubernetesProxyService, ProxyService}
import org.broadinstitute.dsde.workbench.model.google.GoogleProject
import akka.http.scaladsl.model.headers._
import akka.http.scaladsl.server.RouteResult.Complete
import akka.http.scaladsl.server.directives.{DebuggingDirectives, LogEntry, LoggingMagnet}
import cats.effect.{Effect, IO}
import cats.effect.implicits._
import cats.mtl.ApplicativeAsk
import org.broadinstitute.dsde.workbench.leonardo.model.RuntimeAction.ConnectToRuntime
import org.broadinstitute.dsde.workbench.model.{TraceId, UserInfo}
import org.broadinstitute.dsde.workbench.google2.KubernetesSerializableName.ServiceName
import org.broadinstitute.dsde.workbench.leonardo.api.CookieSupport

class ProxyRoutes[F[_]: Effect](proxyService: ProxyService,
                                corsSupport: CorsSupport,
                                kubernetesProxyService: KubernetesProxyService[F])
    extends LazyLogging {
  val route: Route =
    //note that the "notebooks" path prefix is deprecated
    pathPrefix("proxy" | "notebooks") {
      val traceId = TraceId(UUID.randomUUID())
      implicit val traceIO = ApplicativeAsk.const[IO, TraceId](traceId)
      implicit val traceF = ApplicativeAsk.const[F, TraceId](traceId)

      corsSupport.corsHandler {

        pathPrefix(Segment / Segment) { (googleProjectParam, clusterNameParam) =>
          val googleProject = GoogleProject(googleProjectParam)
          val clusterName = RuntimeName(clusterNameParam)

          path("setCookie") {
            extractUserInfo { userInfo =>
              get {
                // Check the user for ConnectToCluster privileges and set a cookie in the response
                onSuccess(
                  proxyService.authCheck(userInfo, googleProject, clusterName, ConnectToRuntime).unsafeToFuture()
                ) {
                  CookieSupport.setTokenCookie(userInfo, CookieSupport.tokenCookieName) {
                    complete {
                      logger.debug(s"Successfully set cookie for user $userInfo")
                      StatusCodes.NoContent
                    }
                  }
                }
              }
            }
          } ~
            pathPrefix("jupyter" | "rstudio") {
              (extractRequest & extractUserInfo) { (request, userInfo) =>
                logRequestResultForMetrics(userInfo) {
                  // Proxy logic handled by the ProxyService class
                  // Note ProxyService calls the LeoAuthProvider internally
                  complete {
                    proxyService.proxyRequest(userInfo, googleProject, clusterName, request)
                  }
                }
              }
            } ~
            pathPrefix(Segment) { serviceNameString =>
              val serviceName = ServiceName(serviceNameString)
              val appName = AppName(clusterNameParam)

              (extractRequest & extractUserInfo) { (request, userInfo) =>
                logRequestResultForMetrics(userInfo) {
                  // Proxy logic handled by the ProxyService class
                  // Note ProxyService calls the LeoAuthProvider internally
                  complete {
                    kubernetesProxyService
                      .proxyRequest(userInfo, googleProject, appName, serviceName, request)
                      .toIO
                  }
                }
              }
            }
        // No need to lookup the user or consult the auth provider for this endpoint
        } ~
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
  private def extractToken: Directive1[String] =
    optionalHeaderValueByType[`Authorization`](()) flatMap {

      // We have an Authorization header, extract the token
      // Note the Authorization header overrides the cookie
      case Some(header) => provide(header.credentials.token)

      // We don't have an Authorization header; check the cookie
      case None =>
        optionalCookie(CookieSupport.tokenCookieName) flatMap {

          // We have a cookie, extract the token
          case Some(cookie) => provide(cookie.value)

          // Not found in cookie or Authorization header, fail
          case None => failWith(AuthenticationError())
        }
    }

  /**
   * Extracts the user token from the request, and looks up the cached UserInfo.
   */
  private def extractUserInfo: Directive1[UserInfo] =
    extractToken.flatMap(token => onSuccess(proxyService.getCachedUserInfoFromToken(token).unsafeToFuture()))

  // basis for logRequestResult lifted from http://stackoverflow.com/questions/32475471/how-does-one-log-akka-http-client-requests
  private def logRequestResultForMetrics(userInfo: UserInfo): Directive0 = {
    def myMetricsLogger(logger: LoggingAdapter)(req: HttpRequest)(res: Any): Unit = {
      val headers = req.headers
      val headerMap: Map[String, String] = headers.map(header => (header.name(), header.value())).toMap

      val entry = res match {
        case Complete(resp) =>
          LogEntry(
            s"${headerMap.getOrElse("X-Forwarded-For", "0.0.0.0")} ${userInfo.userEmail} " +
              s"${userInfo.userId} [${DateTime.now.toIsoDateTimeString()}] " +
              s""""${req.method.value} ${req.uri} ${req.protocol.value}" """ +
              s"""${resp.status.intValue} ${resp.entity.contentLengthOption
                .getOrElse("-")} ${headerMap.getOrElse("Origin", "-")} """ +
              s"${headerMap.getOrElse("User-Agent", "unknown")}",
            Logging.InfoLevel
          )
        case _ => LogEntry(s"No response - request not complete")
      }
      entry.logTo(logger)
    }
    DebuggingDirectives.logRequestResult(LoggingMagnet(myMetricsLogger))
  }
}
