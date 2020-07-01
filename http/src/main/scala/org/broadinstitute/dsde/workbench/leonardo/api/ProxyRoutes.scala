package org.broadinstitute.dsde.workbench.leonardo
package http
package api

import java.util.UUID

import akka.event.{Logging, LoggingAdapter}
import akka.http.scaladsl.model._
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.{Directive0, Directive1, Route}
import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.dsde.workbench.leonardo.http.service.ProxyService
import akka.http.scaladsl.model.headers._
import akka.http.scaladsl.server.RouteResult.Complete
import akka.http.scaladsl.server.directives.{DebuggingDirectives, LogEntry, LoggingMagnet}
import akka.stream.Materializer
import cats.effect.{ContextShift, IO}
import cats.mtl.ApplicativeAsk
import org.broadinstitute.dsde.workbench.leonardo.model.RuntimeAction.ConnectToRuntime
import org.broadinstitute.dsde.workbench.model.{TraceId, UserInfo}
import cats.implicits._
import org.broadinstitute.dsde.workbench.leonardo.api.CookieSupport

class ProxyRoutes(proxyService: ProxyService, corsSupport: CorsSupport)(
  implicit materializer: Materializer,
  cs: ContextShift[IO]
) extends LazyLogging {
  val route: Route =
    // Note that the "notebooks" path prefix is deprecated
    pathPrefix("proxy" | "notebooks") {
      implicit val traceId = ApplicativeAsk.const[IO, TraceId](TraceId(UUID.randomUUID()))

      corsSupport.corsHandler {

        // "apps" proxy routes
        pathPrefix("google" / "v1" / "apps") {
          pathPrefix(googleProjectSegment / appNameSegment / serviceNameSegment) {
            (googleProject, appName, serviceName) =>
              (extractRequest & extractUserInfo) { (request, userInfo) =>
                logRequestResultForMetrics(userInfo) {
                  complete {
                    proxyService.proxyAppRequest(userInfo, googleProject, appName, serviceName, request).onError {
                      case e =>
                        IO(
                          logger.warn(
                            s"proxy request failed for ${userInfo.userEmail.value} ${googleProject.value} ${appName.value} ${serviceName.value}",
                            e
                          )
                        ) <* IO
                          .fromFuture(IO(request.entity.discardBytes().future))
                    }
                  }
                }
              }
          }
        } ~
          // "runtimes" proxy routes
          pathPrefix(googleProjectSegment / runtimeNameSegment) { (googleProject, runtimeName) =>
            // Note this exists at the top-level /proxy/setCookie as well
            path("setCookie") {
              extractUserInfo { userInfo =>
                get {
                  // Check the user for ConnectToCluster privileges and set a cookie in the response
                  onSuccess(
                    proxyService.authCheck(userInfo, googleProject, runtimeName, ConnectToRuntime).unsafeToFuture()
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
              (extractRequest & extractUserInfo) { (request, userInfo) =>
                logRequestResultForMetrics(userInfo) {
                  // Proxy logic handled by the ProxyService class
                  // Note ProxyService calls the LeoAuthProvider internally
                  complete {
                    // we are discarding the request entity here. we have noticed that PUT requests caused by
                    // saving a notebook when a cluster is stopped correlate perfectly with CPU spikes.
                    // in that scenario, the requests appear to pile up, causing apache to hog CPU.
                    proxyService.proxyRequest(userInfo, googleProject, runtimeName, request).onError {
                      case e =>
                        IO(
                          logger.warn(
                            s"proxy request failed for ${userInfo.userEmail.value} ${googleProject.value} ${runtimeName.asString}",
                            e
                          )
                        ) <* IO
                          .fromFuture(IO(request.entity.discardBytes().future))
                    }
                  }
                }
              }
          } ~
          // Top-level routes
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
          } ~
          path("setCookie") {
            extractUserInfo { userInfo =>
              get {
                CookieSupport.setTokenCookie(userInfo, CookieSupport.tokenCookieName) {
                  complete {
                    logger.debug(s"Successfully set cookie for user $userInfo")
                    StatusCodes.NoContent
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
