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
import io.circe.{Decoder, Encoder}
import io.opencensus.scala.akka.http.TracingDirective.traceRequestForService
import org.broadinstitute.dsde.workbench.leonardo.http.api.AdminRoutes._
import org.broadinstitute.dsde.workbench.leonardo.http.service.AdminService
import org.broadinstitute.dsde.workbench.leonardo.JsonCodec._
import org.broadinstitute.dsde.workbench.model.google.GoogleProject
import org.broadinstitute.dsde.workbench.model.{TraceId, UserInfo}
import org.broadinstitute.dsp.ChartVersion

import java.util.UUID

/**
  * Routes intended to be used by Terra admins to manage system state
  */
class AdminRoutes(adminService: AdminService[IO], userInfoDirectives: UserInfoDirectives) {

  val routes: server.Route = traceRequestForService(serviceData) { span =>
    extractAppContext(Some(span)) { implicit ctx =>
      userInfoDirectives.requireUserInfo { userInfo =>
        CookieSupport.setTokenCookie(userInfo) {
          implicit val traceId: Ask[IO, TraceId] = Ask.const[IO, TraceId](TraceId(UUID.randomUUID()))
          pathPrefix("admin" / "v2" / "apps" / "update") {
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

  private[api] def updateAppsHandler(userInfo: UserInfo, req: UpdateAppsRequest)(implicit
    ev: Ask[IO, AppContext]
  ): IO[ToResponseMarshallable] = for {
    ctx <- ev.ask[AppContext]
    apiCall = adminService.updateApps(userInfo, req)
    resp <- ctx.span.fold(apiCall)(span => spanResource[IO](span, "updateApps").use(_ => apiCall))
    retCode = if (req.dryRun) StatusCodes.OK else StatusCodes.Accepted
  } yield retCode -> resp
}

object AdminRoutes {

  implicit val chartVersionDecoder: Decoder[ChartVersion] = Decoder.decodeString.map(ChartVersion)

  implicit val updateAppJobIdDecoder: Decoder[UpdateAppJobId] = Decoder.decodeUUID.map(UpdateAppJobId)

  implicit val updateAppsDecoder: Decoder[UpdateAppsRequest] =
    Decoder.instance { x =>
      for {
        jobId <- x.downField("jobId").as[Option[UpdateAppJobId]]
        at <- x.downField("appType").as[AppType]
        cp <- x.downField("cloudProvider").as[CloudProvider]
        avi <- x.downField("appVersionsInclude").as[Option[List[ChartVersion]]].map(_.getOrElse(List()))
        ave <- x.downField("appVersionsExclude").as[Option[List[ChartVersion]]].map(_.getOrElse(List()))
        gp <- x.downField("googleProject").as[Option[GoogleProject]]
        wid <- x.downField("workspaceId").as[Option[WorkspaceId]]
        aids <- x.downField("appNames").as[Option[List[AppName]]].map(_.getOrElse(List()))
        dr <- x.downField("dryRun").as[Option[Boolean]].map(_.getOrElse(false))
      } yield UpdateAppsRequest(jobId, at, cp, avi, ave, gp, wid, aids, dr)
    }

  implicit val appIdEncoder: Encoder[AppId] = Encoder.encodeLong.contramap(_.id)
  implicit val chartEncoder: Encoder[Chart] = Encoder.encodeString.contramap(_.toString)

  implicit val listUpdateableAppsResponseEncoder: Encoder[ListUpdateableAppResponse] =
    Encoder.forProduct10(
      "workspaceId",
      "cloudContext",
      "status",
      "appId",
      "appName",
      "appType",
      "auditInfo",
      "chart",
      "accessScope",
      "labels"
    )(x =>
      (x.workspaceId,
       x.cloudContext,
       x.status,
       x.appId,
       x.appName,
       x.appType,
       x.auditInfo,
       x.chart,
       x.accessScope,
       x.labels
      )
    )
}
