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
import io.circe.Decoder
import io.opencensus.scala.akka.http.TracingDirective.traceRequestForService
import org.broadinstitute.dsde.workbench.leonardo.http.api.AdminRoutes._
import org.broadinstitute.dsde.workbench.leonardo.http.service.AdminService
import org.broadinstitute.dsde.workbench.model.{TraceId, UserInfo}
//import org.broadinstitute.dsde.workbench.openTelemetry.OpenTelemetryMetrics

import java.util.UUID

class AdminRoutes(adminService: AdminService[IO], userInfoDirectives: UserInfoDirectives)/*(implicit metrics: OpenTelemetryMetrics[IO])*/ {

  val routes: server.Route = traceRequestForService(serviceData) { span =>
    extractAppContext(Some(span)) { implicit ctx =>
      userInfoDirectives.requireUserInfo { userInfo =>
        CookieSupport.setTokenCookie(userInfo) {
          implicit val traceId = Ask.const[IO, TraceId](TraceId(UUID.randomUUID()))
          pathPrefix("admin" / "v2" / "apps" / "update" ) {
            pathEndOrSingleSlash {
              post {
                entity(as[UpdateAppsRequest]) { req =>
                  complete(
                    updateAppsHandler(userInfo, req)
                  )
                }
              }
            }
          }
        }
      }
    }
  }

  private[api] def updateAppsHandler(userInfo: UserInfo,
                                     req: UpdateAppsRequest
  )(implicit
    ev: Ask[IO, AppContext]
  ): IO[ToResponseMarshallable] = for {
    ctx <- ev.ask[AppContext]
    apiCall = adminService.updateApps(userInfo, req)
    // TODO metrics?
    resp <- ctx.span.fold(apiCall)(span => spanResource[IO](span, "updateApps").use(_ => apiCall))
  } yield StatusCodes.Accepted -> resp
}

object AdminRoutes {

  implicit val updateAppsDecoder: Decoder[UpdateAppsRequest] =
    Decoder.instance { x =>
      for {
        d <- x.downField("dryRun").as[Boolean]
      } yield UpdateAppsRequest(d)
    }
}