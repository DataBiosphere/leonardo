package org.broadinstitute.dsde.workbench.leonardo.http
package api

import akka.event.{Logging, LoggingAdapter}
import akka.http.scaladsl.marshalling.ToResponseMarshallable
import akka.http.scaladsl.model._
import akka.http.scaladsl.model.headers._
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.RouteResult.Complete
import akka.http.scaladsl.server.directives.{DebuggingDirectives, LogEntry, LoggingMagnet}
import akka.http.scaladsl.server.{Directive0, Directive1, Route}
import akka.stream.Materializer
import cats.effect.{ContextShift, IO}
import cats.syntax.all._
import cats.mtl.Ask
import org.broadinstitute.dsde.workbench.leonardo.model.AuthenticationError
import com.typesafe.scalalogging.LazyLogging
import io.opencensus.scala.akka.http.TracingDirective.traceRequestForService
import org.broadinstitute.dsde.workbench.google2.KubernetesSerializableName.ServiceName
import org.broadinstitute.dsde.workbench.leonardo.config.RefererConfig
import org.broadinstitute.dsde.workbench.leonardo.{AppContext, AppName, RuntimeName}
import org.broadinstitute.dsde.workbench.leonardo.dao.TerminalName
import org.broadinstitute.dsde.workbench.leonardo.http.service.ProxyService
import org.broadinstitute.dsde.workbench.model.UserInfo
import org.broadinstitute.dsde.workbench.model.google.GoogleProject
import org.broadinstitute.dsde.workbench.openTelemetry.OpenTelemetryMetrics

class ProxyRoutes(proxyService: ProxyService, corsSupport: CorsSupport, refererConfig: RefererConfig)(
  implicit materializer: Materializer,
  cs: ContextShift[IO],
  metrics: OpenTelemetryMetrics[IO]
) extends LazyLogging {
  val route: Route =
    traceRequestForService(serviceData) { span =>
      extractAppContext(Some(span)) { implicit ctx =>
        // Note that the "notebooks" path prefix is deprecated
        pathPrefix("proxy" | "notebooks") {

          corsSupport.corsHandler {

            refererHandler {
              // "apps" proxy routes
              pathPrefix("google" / "v1" / "apps") {
                pathPrefix(googleProjectSegment / appNameSegment / serviceNameSegment) {
                  (googleProject, appName, serviceName) =>
                    (extractRequest & extractUserInfo) { (request, userInfo) =>
                      logRequestResultForMetrics(userInfo) {
                        complete {
                          proxyAppHandler(userInfo, googleProject, appName, serviceName, request)
                        }
                      }
                    }
                }
              } ~
                // "runtimes" proxy routes
                pathPrefix(googleProjectSegment / runtimeNameSegment) { (googleProject, runtimeName) =>
                  // Note the setCookie route exists at the top-level /proxy/setCookie as well
                  path("setCookie") {
                    extractUserInfoFromHeader { userInfoOpt =>
                      get {
                        val cookieDirective = userInfoOpt match {
                          case Some(userInfo) => CookieSupport.setTokenCookie(userInfo, CookieSupport.tokenCookieName)
                          case None           => CookieSupport.unsetTokenCookie(CookieSupport.tokenCookieName)
                        }
                        cookieDirective {
                          complete {
                            setCookieHandler(userInfoOpt)
                          }
                        }
                      }
                    }
                  } ~
                    pathPrefix("jupyter" / "terminals") {
                      pathSuffix(terminalNameSegment) { terminalName =>
                        (extractRequest & extractUserInfo) { (request, userInfo) =>
                          logRequestResultForMetrics(userInfo) {
                            complete {
                              openTerminalHandler(userInfo, googleProject, runtimeName, terminalName, request)
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
                          proxyRuntimeHandler(userInfo, googleProject, runtimeName, request)
                        }
                      }
                    }
                } ~
                // Top-level routes
                path("invalidateToken") {
                  get {
                    extractUserInfoOpt { userInfoOpt =>
                      CookieSupport.unsetTokenCookie(CookieSupport.tokenCookieName) {
                        complete {
                          invalidateTokenHandler(userInfoOpt)
                        }
                      }
                    }
                  }
                } ~
                path("setCookie") {
                  extractUserInfoFromHeader { userInfoOpt =>
                    get {
                      val cookieDirective = userInfoOpt match {
                        case Some(userInfo) => CookieSupport.setTokenCookie(userInfo, CookieSupport.tokenCookieName)
                        case None           => CookieSupport.unsetTokenCookie(CookieSupport.tokenCookieName)
                      }
                      cookieDirective {
                        complete {
                          setCookieHandler(userInfoOpt)
                        }
                      }
                    }
                  }
                }
            }
          }
        }
      }
    }

  /**
   * Extracts the user bearer token from an Authorization header.
   */
  private def extractTokenFromHeader: Directive1[Option[String]] =
    optionalHeaderValueByType(Authorization).map(headerOpt => headerOpt.map(h => h.credentials.token))

  /**
   * Extracts the user bearer token from a LeoToken cookie.
   */
  private def extractTokenFromCookie: Directive1[Option[String]] =
    optionalCookie(CookieSupport.tokenCookieName).map(cookieOpt => cookieOpt.map(c => c.value))

  /**
   * Extracts user info from an Authorization header or LeoToken cookie.
   * Returns None if a token cannot be retrieved.
   */
  private def extractUserInfoOpt: Directive1[Option[UserInfo]] =
    (extractTokenFromHeader orElse extractTokenFromCookie).flatMap {
      case Some(token) => onSuccess(proxyService.getCachedUserInfoFromToken(token).unsafeToFuture()).map(_.some)
      case None        => provide(None)
    }

  /**
   * Like extractUserInfoOpt, but fails with AuthenticationError if a token cannot be retrieved.
   */
  private def extractUserInfo: Directive1[UserInfo] =
    (extractTokenFromHeader orElse extractTokenFromCookie).flatMap {
      case Some(token) => onSuccess(proxyService.getCachedUserInfoFromToken(token).unsafeToFuture())
      case None        => failWith(AuthenticationError())
    }

  /**
   * Extracts user info from an Authorization header _only_.
   * Returns None if a token cannot be retrieved.
   */
  private def extractUserInfoFromHeader: Directive1[Option[UserInfo]] =
    extractTokenFromHeader flatMap {
      case Some(token) => onSuccess(proxyService.getCachedUserInfoFromToken(token).unsafeToFuture()).map(_.some)
      case None        => provide(None)
    }

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

  private[api] def proxyAppHandler(
    userInfo: UserInfo,
    googleProject: GoogleProject,
    appName: AppName,
    serviceName: ServiceName,
    request: HttpRequest
  )(implicit ev: Ask[IO, AppContext]): IO[ToResponseMarshallable] =
    for {
      ctx <- ev.ask[AppContext]
      apiCall = proxyService
        .proxyAppRequest(userInfo, googleProject, appName, serviceName, request)
        .onError {
          case e =>
            IO(
              logger.warn(
                s"${ctx.traceId} | proxy request failed for ${userInfo.userEmail.value} ${googleProject.value} ${appName.value} ${serviceName.value}",
                e
              )
            ) <* IO
              .fromFuture(IO(request.entity.discardBytes().future))
        }
      _ <- metrics.incrementCounter("proxy", tags = Map("action" -> "requestApp"))

      resp <- ctx.span.fold(apiCall)(span => spanResource[IO](span, "proxyApp").use(_ => apiCall))
    } yield resp

  private[api] def openTerminalHandler(
    userInfo: UserInfo,
    googleProject: GoogleProject,
    runtimeName: RuntimeName,
    terminalName: TerminalName,
    request: HttpRequest
  )(implicit ev: Ask[IO, AppContext]): IO[ToResponseMarshallable] =
    for {
      ctx <- ev.ask[AppContext]
      apiCall = proxyService.openTerminal(userInfo, googleProject, runtimeName, terminalName, request)
      _ <- metrics.incrementCounter("proxy", tags = Map("action" -> "openTerminal"))
      resp <- ctx.span.fold(apiCall)(span => spanResource[IO](span, "openTerminal").use(_ => apiCall))
    } yield resp

  private[api] def proxyRuntimeHandler(
    userInfo: UserInfo,
    googleProject: GoogleProject,
    runtimeName: RuntimeName,
    request: HttpRequest
  )(implicit ev: Ask[IO, AppContext]): IO[ToResponseMarshallable] =
    for {
      ctx <- ev.ask[AppContext]
      apiCall = proxyService.proxyRequest(userInfo, googleProject, runtimeName, request)
      _ <- metrics.incrementCounter("proxy", tags = Map("action" -> "runtimeRequest"))
      resp <- ctx.span.fold(apiCall)(span => spanResource[IO](span, "proxyRuntime").use(_ => apiCall))
    } yield resp

  private[api] def setCookieHandler(userInfoOpt: Option[UserInfo]): IO[ToResponseMarshallable] =
    metrics.incrementCounter("proxy", tags = Map("action" -> "setCookie")) >>
      IO(logger.debug(s"Successfully set cookie for user $userInfoOpt"))
        .as(StatusCodes.NoContent)

  private[api] def invalidateTokenHandler(userInfoOpt: Option[UserInfo]): IO[ToResponseMarshallable] =
    metrics.incrementCounter("proxy", tags = Map("action" -> "invalidateToken")) >>
      userInfoOpt.traverse(userInfo => proxyService.invalidateAccessToken(userInfo.accessToken.token)) >>
      IO(logger.debug(s"Invalidated access token"))
        .as(StatusCodes.NoContent)

  private[api] def refererHandler: Directive0 =
    if (refererConfig.enabled) {
      optionalHeaderValueByType(Upgrade) flatMap {
        case Some(upgrade) if upgrade.hasWebSocket => pass
        case _                                     => checkReferer
      }
    } else {
      pass
    }

  private def requestPath(req: HttpRequest): String = req.uri.toString
  private val logRequestPath = DebuggingDirectives.logRequest(requestPath _)

  private[api] def checkReferer: Directive0 =
    optionalHeaderValueByType(Referer) flatMap {
      case Some(referer) => {
        if (refererConfig.validHosts.contains(referer.uri.authority.toString())) pass
        else {
          logger.info(s"Referer ${referer.uri.toString} is not allowed")
          logRequestPath.tflatMap(_ => failWith(AuthenticationError()))
        }
      }
      case None => {
        logger.info(s"Referer header is missing")
        logRequestPath.tflatMap(_ => failWith(AuthenticationError()))
      }
    }
}
