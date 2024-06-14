package org.broadinstitute.dsde.workbench.leonardo
package http
package api

import cats.syntax.all._
import akka.http.scaladsl.marshalling.ToResponseMarshallable
import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server
import akka.http.scaladsl.server.Directives.{pathEndOrSingleSlash, _}
import cats.effect.IO
import cats.mtl.Ask
import de.heikoseeberger.akkahttpcirce.ErrorAccumulatingCirceSupport._
import io.circe.{Decoder, DecodingFailure, Encoder, KeyEncoder}
import io.opencensus.scala.akka.http.TracingDirective.traceRequestForService
import org.broadinstitute.dsde.workbench.google2.KubernetesSerializableName.ServiceName
import org.broadinstitute.dsde.workbench.leonardo.JsonCodec._
import org.broadinstitute.dsde.workbench.leonardo.http.api.AppV2Routes._
import org.broadinstitute.dsde.workbench.leonardo.http.service.AppService
import org.broadinstitute.dsde.workbench.model.UserInfo
import org.broadinstitute.dsde.workbench.openTelemetry.OpenTelemetryMetrics
import org.http4s.Uri

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
              RouteValidation.validateNameDirective(appNameString, AppName.apply) { appName =>
                post {
                  entity(as[CreateAppRequest]) { req =>
                    complete(
                      createAppV2Handler(userInfo, workspaceId, appName, req)
                    )
                  }
                } ~
                  get {
                    complete(
                      getAppV2Handler(userInfo, workspaceId, appName)
                    )
                  } ~
                  delete {
                    parameterMap { params =>
                      complete(
                        deleteAppV2Handler(userInfo, workspaceId, appName, params)
                      )
                    }
                  }
              }
            } ~ pathPrefix("deleteAll") {
              post {
                parameterMap { params =>
                  complete(
                    deleteAllAppsForWorkspaceHandler(userInfo, workspaceId, params)
                  )
                }
              }
            }
          }
        }
      }
    }
  }

  private[api] def createAppV2Handler(userInfo: UserInfo,
                                      workspaceId: WorkspaceId,
                                      appName: AppName,
                                      req: CreateAppRequest
  )(implicit ev: Ask[IO, AppContext]): IO[ToResponseMarshallable] = {
    val apiCallName = "createAppV2"
    for {
      _ <- metrics.incrementCounter(apiCallName)
      _ <- withSpanResource(apiCallName, kubernetesService.createAppV2(userInfo, workspaceId, appName, req))
    } yield StatusCodes.Accepted
  }

  private[api] def getAppV2Handler(userInfo: UserInfo, workspaceId: WorkspaceId, appName: AppName)(implicit
    ev: Ask[IO, AppContext]
  ): IO[ToResponseMarshallable] = {
    val apiCallName = "getAppV2"
    for {
      _ <- metrics.incrementCounter(apiCallName)
      resp <- withSpanResource(apiCallName,
                               kubernetesService.getAppV2(
                                 userInfo,
                                 workspaceId,
                                 appName
                               )
      )
    } yield StatusCodes.OK -> resp
  }

  private[api] def listAppV2Handler(userInfo: UserInfo, workspaceId: WorkspaceId, params: Map[String, String])(implicit
    ev: Ask[IO, AppContext]
  ): IO[ToResponseMarshallable] = {
    val apiCallName = "listAppV2"
    for {
      _ <- metrics.incrementCounter(apiCallName)
      resp <- withSpanResource(apiCallName,
                               kubernetesService.listAppV2(
                                 userInfo,
                                 workspaceId,
                                 params
                               )
      )
    } yield StatusCodes.OK -> resp
  }

  private[api] def deleteAppV2Handler(userInfo: UserInfo,
                                      workspaceId: WorkspaceId,
                                      appName: AppName,
                                      params: Map[String, String]
  )(implicit ev: Ask[IO, AppContext]): IO[ToResponseMarshallable] = {
    val apiCallName = "deleteAppV2"
    val deleteDisk = params.get("deleteDisk").exists(_ == "true")
    val tags = Map("deleteDisk" -> deleteDisk.toString)
    for {
      _ <- metrics.incrementCounter(apiCallName, 1, tags)
      _ <- withSpanResource(apiCallName,
                            kubernetesService.deleteAppV2(
                              userInfo,
                              workspaceId,
                              appName,
                              deleteDisk
                            )
      )
    } yield StatusCodes.Accepted
  }

  private[api] def deleteAllAppsForWorkspaceHandler(userInfo: UserInfo,
                                                    workspaceId: WorkspaceId,
                                                    params: Map[String, String]
  )(implicit ev: Ask[IO, AppContext]): IO[ToResponseMarshallable] = {
    val apiCallName = "deleteAllAppV2"
    val deleteDisk = params.get("deleteDisk").exists(_ == "true")
    val tags = Map("deleteDisk" -> deleteDisk.toString)
    for {
      _ <- metrics.incrementCounter(apiCallName, 1, tags)
      _ <- withSpanResource(apiCallName,
                            kubernetesService.deleteAllAppsV2(
                              userInfo,
                              workspaceId,
                              deleteDisk
                            )
      )
    } yield StatusCodes.Accepted
  }

}

object AppV2Routes {

  implicit val createAppDecoder: Decoder[CreateAppRequest] =
    Decoder.instance { x =>
      for {
        c <- x.downField("kubernetesRuntimeConfig").as[Option[KubernetesRuntimeConfig]]
        s <- x.downField("accessScope").as[Option[AppAccessScope]]
        d <- x.downField("diskConfig").as[Option[PersistentDiskRequest]]
        l <- x.downField("labels").as[Option[LabelMap]]
        cv <- x.downField("customEnvironmentVariables").as[Option[LabelMap]]
        dp <- x.downField("descriptorPath").as[Option[Uri]]
        ea <- x.downField("extraArgs").as[Option[List[String]]]
        wsi <- x.downField("workspaceId").as[Option[WorkspaceId]]
        swi <- x.downField("sourceWorkspaceId").as[Option[WorkspaceId]]
        adte <- x.downField("autodeleteEnabled").as[Option[Boolean]]
        adtm <- x.downField("autodeleteThreshold").as[Option[AutodeleteThreshold]]
        autopilot <- x.downField("autopilot").as[Option[Autopilot]]
        mountBucketName <- x.downField("mountWorkspaceBucketName").as[Option[String]]

        optStr <- x.downField("appType").as[Option[String]]
        cn <- x.downField("allowedChartName").as[Option[AllowedChartName]]
        // TODO: once AOU has migrated to use the new app type, we can use much simpler version instead of this workaround for backwards compatibility
        (appType, allowedChartName) <- optStr match {
          case Some(value) =>
            AppType.stringToObject
              .get(value) match {
              case Some(v) => (v, cn).asRight[DecodingFailure]
              case None =>
                if (value == "RSTUDIO")
                  (AppType.Allowed, Some(AllowedChartName.RStudio)).asRight[DecodingFailure]
                else
                  DecodingFailure(s"Invalid app type ${value}", List.empty).asLeft[(AppType, Option[AllowedChartName])]
            }
          case None => (AppType.Galaxy, cn).asRight[DecodingFailure]
        }
      } yield CreateAppRequest(
        c,
        appType,
        allowedChartName,
        s,
        d,
        l.getOrElse(Map.empty),
        cv.getOrElse(Map.empty),
        dp,
        ea.getOrElse(List.empty),
        wsi,
        swi,
        adte,
        adtm,
        autopilot,
        mountBucketName
      )
    }

  implicit val nameKeyEncoder: KeyEncoder[ServiceName] = KeyEncoder.encodeKeyString.contramap(_.value)
  implicit val listAppResponseEncoder: Encoder[ListAppResponse] =
    Encoder.forProduct16(
      "workspaceId",
      "cloudContext",
      "region",
      "kubernetesRuntimeConfig",
      "errors",
      "status",
      "proxyUrls",
      "appName",
      "appType",
      "chartName",
      "diskName",
      "auditInfo",
      "accessScope",
      "labels",
      "autodeleteEnabled",
      "autodeleteThreshold"
    )(x =>
      (x.workspaceId,
       x.cloudContext,
       x.region,
       x.kubernetesRuntimeConfig,
       x.errors,
       x.status,
       x.proxyUrls,
       x.appName,
       x.appType,
       x.chartName,
       x.diskName,
       x.auditInfo,
       x.accessScope,
       x.labels,
       x.autodeleteEnabled,
       x.autodeleteThreshold
      )
    )

  implicit val getAppResponseEncoder: Encoder[GetAppResponse] =
    Encoder.forProduct17(
      "workspaceId",
      "appName",
      "cloudContext",
      "region",
      "kubernetesRuntimeConfig",
      "errors",
      "status",
      "proxyUrls",
      "diskName",
      "customEnvironmentVariables",
      "auditInfo",
      "appType",
      "chartName",
      "accessScope",
      "labels",
      "autodeleteEnabled",
      "autodeleteThreshold"
    )(x => GetAppResponse.unapply(x).get)
}
