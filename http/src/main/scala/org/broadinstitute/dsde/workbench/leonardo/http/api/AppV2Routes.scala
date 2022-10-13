package org.broadinstitute.dsde.workbench.leonardo.http.api

import akka.http.scaladsl.marshalling.ToResponseMarshallable
import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server
import akka.http.scaladsl.server.Directives.{pathEndOrSingleSlash, _}
import cats.effect.IO
import cats.mtl.Ask
import io.circe.{Encoder, KeyEncoder}
import io.opencensus.scala.akka.http.TracingDirective.traceRequestForService
import org.broadinstitute.dsde.workbench.google2.KubernetesSerializableName.ServiceName
import org.broadinstitute.dsde.workbench.leonardo.JsonCodec._
import org.broadinstitute.dsde.workbench.leonardo.http.service.AppService
import org.broadinstitute.dsde.workbench.leonardo.http.{serviceData, spanResource, ListAppV2Response}
import org.broadinstitute.dsde.workbench.leonardo.{AppContext, WorkspaceId}
import org.broadinstitute.dsde.workbench.model.UserInfo
import org.broadinstitute.dsde.workbench.openTelemetry.OpenTelemetryMetrics

class AppV2Routes(kubernetesService: AppService[IO], userInfoDirectives: UserInfoDirectives)(implicit
  metrics: OpenTelemetryMetrics[IO]
) {
  val routes: server.Route = traceRequestForService(serviceData) { span =>
    extractAppContext(Some(span)) { implicit ctx =>
      userInfoDirectives.requireUserInfo { userInfo =>
        CookieSupport.setTokenCookie(userInfo) {
          pathPrefix("apps" / "v2" / workspaceIdSegment) { workspaceId =>
            pathEndOrSingleSlash {
              parameterMap { params =>
                get {
                  complete(
                    listAppV2Handler(userInfo, workspaceId, params)
                  )
                }
              }
            } ~ pathPrefix(Segment) { appNameString =>
              post {
                ???
              } ~
                get {
                  ???
                } ~
                delete {
                  ???
                }

            }
          }
        }
      }
    }
  }

  private[api] def listAppV2Handler(userInfo: UserInfo, workspaceId: WorkspaceId, params: Map[String, String])(implicit
    ev: Ask[IO, AppContext]
  ): IO[ToResponseMarshallable] =
    for {
      ctx <- ev.ask[AppContext]
      apiCall = kubernetesService.listAppV2(
        userInfo,
        workspaceId,
        params
      )
      _ <- metrics.incrementCounter("listApp")
      resp <- ctx.span.fold(apiCall)(span => spanResource[IO](span, "listAppV2").use(_ => apiCall))
    } yield StatusCodes.OK
}

object AppV2Routes {

  implicit val nameKeyEncoder: KeyEncoder[ServiceName] = KeyEncoder.encodeKeyString.contramap(_.value)
  implicit val listAppV2ResponseEncoder: Encoder[ListAppV2Response] =
    Encoder.forProduct11(
      "cloudProvider",
      "workspaceId",
      "kubernetesRuntimeConfig",
      "errors",
      "status",
      "proxyUrls",
      "appName",
      "appType",
      "diskName",
      "auditInfo",
      "labels"
    )(x =>
      (x.cloudProvider,
       x.workspaceId,
       x.kubernetesRuntimeConfig,
       x.errors,
       x.status,
       x.proxyUrls,
       x.appName,
       x.appType,
       x.diskName,
       x.auditInfo,
       x.labels
      )
    )
}
