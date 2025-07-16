package org.broadinstitute.dsde.workbench.leonardo
package http
package api

import akka.http.scaladsl.marshalling.ToResponseMarshallable
import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server
import akka.http.scaladsl.server.Directives._
import cats.effect.IO
import cats.mtl.Ask
import de.heikoseeberger.akkahttpcirce.ErrorAccumulatingCirceSupport._
import io.opencensus.scala.akka.http.TracingDirective.traceRequestForService
import org.broadinstitute.dsde.workbench.leonardo.http.RuntimeRoutesCodec._
import org.broadinstitute.dsde.workbench.leonardo.http.service.RuntimeV2Service
import org.broadinstitute.dsde.workbench.model.UserInfo
import org.broadinstitute.dsde.workbench.openTelemetry.OpenTelemetryMetrics

class RuntimeV2Routes(runtimeV2Service: RuntimeV2Service[IO], userInfoDirectives: UserInfoDirectives)(implicit
  metrics: OpenTelemetryMetrics[IO]
) {

  val routes: server.Route = traceRequestForService(serviceData) { span =>
    extractAppContext(Some(span)) { implicit ctx =>
      userInfoDirectives.requireUserInfo { userInfo =>
        CookieSupport.setTokenCookie(userInfo) {
          pathPrefix("v2" / "runtimes") {
            pathEndOrSingleSlash {
              parameterMap { params =>
                get {
                  complete(
                    listRuntimesHandler(
                      userInfo,
                      None,
                      None,
                      params
                    )
                  )
                }
              }
            }
          } ~
            pathPrefix(workspaceIdSegment) { workspaceId =>
              pathPrefix(runtimeNameSegmentWithValidation) { runtimeName =>
                path("stop") {
                  post {
                    complete(
                      stopRuntimeHandler(
                        userInfo,
                        workspaceId,
                        runtimeName
                      )
                    )
                  }
                } ~
                  path("start") {
                    post {
                      complete(
                        startRuntimeHandler(
                          userInfo,
                          workspaceId,
                          runtimeName
                        )
                      )
                    }
                  }
              }
            }
        }
      }
    }
  }

  private[api] def listRuntimesHandler(userInfo: UserInfo,
                                       workspaceId: Option[WorkspaceId],
                                       cloudProvider: Option[CloudProvider],
                                       params: Map[String, String]
  )(implicit
    ev: Ask[IO, AppContext]
  ): IO[ToResponseMarshallable] =
    for {
      ctx <- ev.ask[AppContext]
      apiCall = runtimeV2Service.listRuntimes(userInfo, workspaceId, cloudProvider, params)
      _ <- metrics.incrementCounter("listRuntimeV2")
      resp <- ctx.span.fold(apiCall)(span =>
        spanResource[IO](span, "listRuntimeV2")
          .use(_ => apiCall)
      )
    } yield StatusCodes.OK -> resp: ToResponseMarshallable

  private[api] def startRuntimeHandler(userInfo: UserInfo, workspaceId: WorkspaceId, runtimeName: RuntimeName)(implicit
    ev: Ask[IO, AppContext]
  ): IO[ToResponseMarshallable] =
    for {
      ctx <- ev.ask[AppContext]
      apiCall = runtimeV2Service.startRuntime(userInfo, runtimeName, workspaceId)
      _ <- metrics.incrementCounter("startRuntimeV2")
      resp <- ctx.span.fold(apiCall)(span =>
        spanResource[IO](span, "startRuntimeV2")
          .use(_ => apiCall)
      )
    } yield StatusCodes.Accepted -> resp: ToResponseMarshallable

  private[api] def stopRuntimeHandler(userInfo: UserInfo, workspaceId: WorkspaceId, runtimeName: RuntimeName)(implicit
    ev: Ask[IO, AppContext]
  ): IO[ToResponseMarshallable] =
    for {
      ctx <- ev.ask[AppContext]
      apiCall = runtimeV2Service.stopRuntime(userInfo, runtimeName, workspaceId)
      _ <- metrics.incrementCounter("stopRuntimeV2")
      resp <- ctx.span.fold(apiCall)(span =>
        spanResource[IO](span, "stopRuntimeV2")
          .use(_ => apiCall)
      )
    } yield StatusCodes.Accepted -> resp: ToResponseMarshallable
}
