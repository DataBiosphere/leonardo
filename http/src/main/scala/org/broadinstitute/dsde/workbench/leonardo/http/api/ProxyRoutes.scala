package org.broadinstitute.dsde.workbench.leonardo
package http
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
import cats.effect.IO
import cats.mtl.Ask
import cats.syntax.all._
import com.typesafe.scalalogging.LazyLogging
import io.opencensus.scala.akka.http.TracingDirective.traceRequestForService
import org.broadinstitute.dsde.workbench.google2.KubernetesSerializableName.ServiceName
import org.broadinstitute.dsde.workbench.leonardo.config.RefererConfig
import org.broadinstitute.dsde.workbench.leonardo.dao.TerminalName
import org.broadinstitute.dsde.workbench.leonardo.http.service.ProxyService
import org.broadinstitute.dsde.workbench.leonardo.model.AuthenticationError
import org.broadinstitute.dsde.workbench.model.google.GoogleProject
import org.broadinstitute.dsde.workbench.model.{TraceId, UserInfo}
import org.broadinstitute.dsde.workbench.openTelemetry.OpenTelemetryMetrics

class ProxyRoutes(proxyService: ProxyService, corsSupport: CorsSupport, refererConfig: RefererConfig)(implicit
  materializer: Materializer,
  metrics: OpenTelemetryMetrics[IO]
) extends LazyLogging {
  val route: Route =
    traceRequestForService(serviceData) { span =>
      extractRequest { request =>
        extractAppContext(Some(span), request.uri.toString()) { implicit ctx =>
          // Note that the "notebooks" path prefix is deprecated
          pathPrefix("proxy" | "notebooks") {

            corsSupport.corsHandler {

              refererHandler {
                // "apps" proxy routes
                pathPrefix("google" / "v1" / "apps") {
                  pathPrefix(googleProjectSegment / appNameSegment / serviceNameSegment) {
                    (googleProject, appName, serviceName) =>
                      extractUserInfoWithoutUserEnabledCheck(implicitly) { userInfo =>
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
                      extractUserInfoFromHeaderWithUserEnabledCheck(implicitly) { userInfoOpt =>
                        get {
                          val cookieDirective = userInfoOpt match {
                            case Some(userInfo) => CookieSupport.setTokenCookie(userInfo)
                            case None           => CookieSupport.unsetTokenCookie()
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
                          extractUserInfoWithoutUserEnabledCheck(implicitly) { userInfo =>
                            logRequestResultForMetrics(userInfo) {
                              complete {
                                openTerminalHandler(userInfo, googleProject, runtimeName, terminalName, request)
                              }
                            }
                          }
                        }
                      } ~
                      extractUserInfoWithoutUserEnabledCheck(implicitly) { userInfo =>
                        logRequestResultForMetrics(userInfo) {
                          // Proxy logic handled by the ProxyService class
                          // Note ProxyService calls the LeoAuthProvider internally
                          complete {
                            proxyRuntimeHandler(userInfo, CloudContext.Gcp(googleProject), runtimeName, request)
                          }
                        }
                      }
                  } ~
                  // Top-level routes
                  path("invalidateToken") {
                    get {
                      extractUserInfoOptWithUserEnabledCheck(implicitly) { userInfoOpt =>
                        CookieSupport.unsetTokenCookie() {
                          complete {
                            invalidateTokenHandler(userInfoOpt)
                          }
                        }
                      }
                    }
                  } ~
                  path("setCookie") {
                    extractUserInfoFromHeaderWithUserEnabledCheck(implicitly) { userInfoOpt =>
                      get {
                        val cookieDirective = userInfoOpt match {
                          case Some(userInfo) => CookieSupport.setTokenCookie(userInfo)
                          case None           => CookieSupport.unsetTokenCookie()
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
  private def extractUserInfoOptWithUserEnabledCheck(implicit ev: Ask[IO, TraceId]): Directive1[Option[UserInfo]] =
    (extractTokenFromHeader orElse extractTokenFromCookie).flatMap {
      case Some(token) =>
        onSuccess(
          proxyService.getCachedUserInfoFromToken(token, true).unsafeToFuture()(cats.effect.unsafe.IORuntime.global)
        ).map(_.some)
      case None => provide(None)
    }

  /**
   * Like extractUserInfoOpt, but fails with AuthenticationError if a token cannot be retrieved.
   */
  private def extractUserInfoWithoutUserEnabledCheck(implicit ev: Ask[IO, TraceId]): Directive1[UserInfo] =
    (extractTokenFromHeader orElse extractTokenFromCookie).flatMap {
      case Some(token) =>
        onSuccess(
          proxyService.getCachedUserInfoFromToken(token, false).unsafeToFuture()(cats.effect.unsafe.IORuntime.global)
        )
      case None => failWith(AuthenticationError())
    }

  /**
   * Extracts user info from an Authorization header _only_.
   * Returns None if a token cannot be retrieved.
   */
  private def extractUserInfoFromHeaderWithUserEnabledCheck(implicit
    ev: Ask[IO, TraceId]
  ): Directive1[Option[UserInfo]] =
    extractTokenFromHeader flatMap {
      case Some(token) =>
        onSuccess(
          proxyService.getCachedUserInfoFromToken(token, true).unsafeToFuture()(cats.effect.unsafe.IORuntime.global)
        ).map(_.some)
      case None => provide(None)
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
        .proxyAppRequest(userInfo, CloudContext.Gcp(googleProject), appName, None, serviceName, request)
        .onError { case e =>
          IO(
            logger.warn(
              s"${ctx.traceId} | proxy request failed for ${userInfo.userEmail.value} ${googleProject.value} ${appName.value} ${serviceName.value}",
              e
            )
          ) <* IO
            .fromFuture(IO(request.entity.discardBytes().future))
        }
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
      resp <- ctx.span.fold(apiCall)(span => spanResource[IO](span, "openTerminal").use(_ => apiCall))
    } yield resp

  private[api] def proxyRuntimeHandler(
    userInfo: UserInfo,
    cloudContext: CloudContext,
    runtimeName: RuntimeName,
    request: HttpRequest
  )(implicit ev: Ask[IO, AppContext]): IO[ToResponseMarshallable] =
    for {
      ctx <- ev.ask[AppContext]
      apiCall = proxyService.proxyRequest(userInfo, cloudContext, runtimeName, None, request)
      resp <- ctx.span.fold(apiCall)(span => spanResource[IO](span, "proxyRuntime").use(_ => apiCall))
      _ <-
        if (request.uri.toString.endsWith(".ipynb") && request.method == HttpMethods.PUT) {
          request.entity.contentLengthOption.traverse(size =>
            metrics.gauge("notebooksSize", size.toDouble, tags = Map("source" -> "proxy"))
          )
        } else IO.unit
    } yield resp

  private[api] def setCookieHandler(userInfoOpt: Option[UserInfo]): IO[ToResponseMarshallable] =
    metrics.incrementCounter("proxyAuthRequest", tags = Map("api" -> "setCookie")) >>
      IO(logger.debug(s"Successfully set cookie for user $userInfoOpt"))
        .as(StatusCodes.NoContent)

  private[api] def invalidateTokenHandler(userInfoOpt: Option[UserInfo]): IO[ToResponseMarshallable] =
    metrics.incrementCounter("proxyAuthRequest", tags = Map("api" -> "invalidateToken")) >>
      userInfoOpt.traverse(userInfo => proxyService.invalidateAccessToken(userInfo.accessToken.token)) >>
      IO(logger.debug(s"Invalidated access token"))
        .as(StatusCodes.NoContent)

  private[api] def refererHandler: Directive0 =
    if (refererConfig.enabled) {
      optionalHeaderValueByType(Upgrade) flatMap {
        case Some(upgrade) if upgrade.hasWebSocket => pass
        case _ =>
          extractUri flatMap { uri =>
            if (isStaticAssetPath(uri.path.toString()))
              // Static assets (JS, CSS, images) are GET-only and cannot be exploited via CSRF,
              // so skip the referer check. This is necessary for environments (e.g. dev with GCP IAP)
              // where Sec-Fetch-* headers are stripped by an intermediate proxy before reaching Leo.
              pass
            else
              checkReferer
          }
      }
    } else {
      pass
    }

  // Returns true for paths that serve static assets (by extension or /static/ path segment).
  // These are safe to exempt from CSRF referer checks because they are GET-only, read-only,
  // and cannot modify server state.
  private[api] def isStaticAssetPath(path: String): Boolean = {
    val staticExtensions = Set(".js",
                               ".css",
                               ".png",
                               ".ico",
                               ".woff",
                               ".woff2",
                               ".svg",
                               ".map",
                               ".gif",
                               ".jpg",
                               ".jpeg",
                               ".ttf",
                               ".eot",
                               ".html"
    )
    path.contains("/static/") || staticExtensions.exists(path.endsWith)
  }

  private def requestPath(req: HttpRequest): String = req.uri.toString
  private val logRequestPath = DebuggingDirectives.logRequest(requestPath _)

  private[api] def checkReferer: Directive0 =
    optionalHeaderValueByType(Referer) flatMap {
      case Some(referer) =>
        val authority = referer.uri.authority
        if (
          refererConfig.validHosts.contains(authority.toString())
          || refererConfig.validHosts.contains("*")
        ) {
          pass
        } else {
          logger.info(s"Referer ${referer.uri.toString} is not allowed")
          logRequestPath.tflatMap(_ => failWith(AuthenticationError()))
        }
      case None =>
        // Referer is absent — check the Origin header as a fallback. Browsers send Origin
        // on fetch/module-import requests (e.g. Galaxy visualization plugin assets loaded
        // by analysis.bundled.js) even when Referer is stripped by the referrer policy.
        // Origin provides equivalent CSRF protection: it cannot be forged by HTML forms
        // and is set to the initiating document's origin by the browser.
        optionalHeaderValueByType(`Origin`) flatMap {
          case Some(origin) =>
            val hasValidOrigin = origin.origins.exists { o =>
              refererConfig.validHosts.contains(o.host.toString()) || refererConfig.validHosts.contains("*")
            }
            if (hasValidOrigin) pass
            else {
              logger.info(s"Referer header is missing and Origin ${origin.value} is not allowed")
              logRequestPath.tflatMap(_ => failWith(AuthenticationError()))
            }
          case None =>
            // Neither Referer nor Origin — final fallback: Sec-Fetch-Site.
            // Browsers always populate this header and it cannot be forged by
            // page scripts (Sec-* is a forbidden header prefix).  "same-origin"
            // guarantees the request was initiated by the same origin, so it
            // cannot be a CSRF attack.  This covers no-cors resource loads
            // (e.g. <script> / <link> tags) from pages with a strict referrer
            // policy that strip both Referer and Origin.
            optionalHeaderValueByName("Sec-Fetch-Site") flatMap {
              case Some(site) if site == "same-origin" =>
                pass
              case _ =>
                logger.info(s"Referer header is missing and no valid Origin or Sec-Fetch-Site")
                logRequestPath.tflatMap(_ => failWith(AuthenticationError()))
            }
        }
    }
}
