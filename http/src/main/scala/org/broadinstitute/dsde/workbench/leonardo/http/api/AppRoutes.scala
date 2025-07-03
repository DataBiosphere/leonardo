package org.broadinstitute.dsde.workbench.leonardo
package http
package api

import akka.http.scaladsl.marshalling.ToResponseMarshallable
import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server
import akka.http.scaladsl.server.Directives._
import cats.effect.IO
import cats.implicits.catsSyntaxEitherId
import cats.mtl.Ask
import de.heikoseeberger.akkahttpcirce.ErrorAccumulatingCirceSupport._
import io.circe.{Decoder, DecodingFailure, Encoder, KeyEncoder}
import io.opencensus.scala.akka.http.TracingDirective.traceRequestForService
import org.broadinstitute.dsde.workbench.google2.KubernetesSerializableName.ServiceName
import org.broadinstitute.dsde.workbench.leonardo.JsonCodec._
import org.broadinstitute.dsde.workbench.leonardo.http.api.AppRoutes._
import org.broadinstitute.dsde.workbench.leonardo.http.service.AppService
import org.broadinstitute.dsde.workbench.model.UserInfo
import org.broadinstitute.dsde.workbench.model.google.{GcsBucketName, GoogleProject}
import org.broadinstitute.dsde.workbench.openTelemetry.OpenTelemetryMetrics
import org.http4s.Uri

class AppRoutes(kubernetesService: AppService[IO], userInfoDirectives: UserInfoDirectives)(implicit
  metrics: OpenTelemetryMetrics[IO]
) {
  val routes: server.Route = traceRequestForService(serviceData) { span =>
    extractAppContext(Some(span)) { implicit ctx =>
      userInfoDirectives.requireUserInfo { userInfo =>
        CookieSupport.setTokenCookie(userInfo) {
          pathPrefix("google" / "v1" / "apps") {
            pathEndOrSingleSlash {
              parameterMap { params =>
                get {
                  complete(
                    listAppHandler(userInfo, None, params)
                  )
                }
              }
            } ~
              pathPrefix(googleProjectSegment) { googleProject =>
                pathEndOrSingleSlash {
                  parameterMap { params =>
                    get {
                      complete(
                        listAppHandler(
                          userInfo,
                          Some(googleProject),
                          params
                        )
                      )
                    }
                  }
                } ~
                  pathPrefix(Segment) { appNameString =>
                    RouteValidation.validateNameDirective(appNameString, AppName.apply) { appName =>
                      pathEndOrSingleSlash {
                        post {
                          entity(as[CreateAppRequest]) { req =>
                            complete(
                              createAppHandler(userInfo, googleProject, appName, req)
                            )
                          }
                        } ~
                          get {
                            complete(
                              getAppHandler(
                                userInfo,
                                googleProject,
                                appName
                              )
                            )
                          } ~
                          patch {
                            entity(as[UpdateAppRequest]) { req =>
                              complete(
                                updateAppHandler(userInfo, googleProject, appName, req)
                              )
                            }
                          } ~
                          delete {
                            parameterMap { params =>
                              complete(
                                deleteAppHandler(
                                  userInfo,
                                  googleProject,
                                  appName,
                                  params
                                )
                              )
                            }
                          }
                      } ~
                        path("stop") {
                          post {
                            complete {
                              stopAppHandler(userInfo, googleProject, appName)
                            }
                          }
                        } ~
                        path("start") {
                          post {
                            complete {
                              startAppHandler(userInfo, googleProject, appName)
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

  private[api] def createAppHandler(userInfo: UserInfo,
                                    googleProject: GoogleProject,
                                    appName: AppName,
                                    req: CreateAppRequest
  )(implicit ev: Ask[IO, AppContext]): IO[ToResponseMarshallable] =
    for {
      _ <- req.allowedChartName match {
        case Some(cn) =>
          val tags = Map("appType" -> req.appType.toString) + ("chartName" -> cn.asString)
          metrics.incrementCounter("createAllowedApp",
                                   1,
                                   tags
          ) // Prometheus doesn't support modifying existing labels. Hence create new metrics for ALLOWED app
        case None =>
          val tags = Map("appType" -> req.appType.toString)
          metrics.incrementCounter("createApp", 1, tags)
      }
      _ <- withSpanResource("createApp",
                            kubernetesService.createApp(
                              userInfo,
                              CloudContext.Gcp(googleProject),
                              appName,
                              req
                            )
      )
    } yield StatusCodes.Accepted

  private[api] def getAppHandler(userInfo: UserInfo, googleProject: GoogleProject, appName: AppName)(implicit
    ev: Ask[IO, AppContext]
  ): IO[ToResponseMarshallable] = {
    val apiCallName = "getApp"
    for {
      _ <- metrics.incrementCounter(apiCallName)
      resp <- withSpanResource(apiCallName,
                               kubernetesService.getApp(userInfo, CloudContext.Gcp(googleProject), appName)
      )
    } yield StatusCodes.OK -> resp
  }

  private[api] def listAppHandler(userInfo: UserInfo,
                                  googleProject: Option[GoogleProject],
                                  params: Map[String, String]
  )(implicit ev: Ask[IO, AppContext]): IO[ToResponseMarshallable] = {
    val apiCallName = "listApp"
    for {
      _ <- metrics.incrementCounter(apiCallName)
      resp <- withSpanResource(apiCallName,
                               kubernetesService.listApp(
                                 userInfo,
                                 googleProject.map(CloudContext.Gcp),
                                 params
                               )
      )
    } yield StatusCodes.OK -> resp
  }

  private[api] def updateAppHandler(userInfo: UserInfo,
                                    googleProject: GoogleProject,
                                    appName: AppName,
                                    req: UpdateAppRequest
  )(implicit ev: Ask[IO, AppContext]): IO[ToResponseMarshallable] =
    for {
      _ <- withSpanResource("updateApp",
                            kubernetesService.updateApp(
                              userInfo,
                              CloudContext.Gcp(googleProject),
                              appName,
                              req
                            )
      )
    } yield StatusCodes.Accepted

  private[api] def deleteAppHandler(userInfo: UserInfo,
                                    googleProject: GoogleProject,
                                    appName: AppName,
                                    params: Map[String, String]
  )(implicit ev: Ask[IO, AppContext]): IO[ToResponseMarshallable] = for {
    _ <- withSpanResource("deleteApp",
                          kubernetesService.deleteApp(
                            userInfo,
                            CloudContext.Gcp(googleProject),
                            appName,
                            deleteDisk = params.get("deleteDisk").exists(_ == "true")
                          )
    )
  } yield StatusCodes.Accepted

  private[api] def stopAppHandler(userInfo: UserInfo, googleProject: GoogleProject, appName: AppName)(implicit
    ev: Ask[IO, AppContext]
  ): IO[ToResponseMarshallable] =
    for {
      _ <- withSpanResource("stopApp", kubernetesService.stopApp(userInfo, CloudContext.Gcp(googleProject), appName))
    } yield StatusCodes.Accepted

  private[api] def startAppHandler(userInfo: UserInfo, googleProject: GoogleProject, appName: AppName)(implicit
    ev: Ask[IO, AppContext]
  ): IO[ToResponseMarshallable] =
    for {
      _ <- withSpanResource("startApp", kubernetesService.startApp(userInfo, CloudContext.Gcp(googleProject), appName))
    } yield StatusCodes.Accepted
}

object AppRoutes {
  implicit val numNodepoolsDecoder: Decoder[NumNodepools] = Decoder.decodeInt.emap(n =>
    n match {
      case n if n < 1   => Left("Minimum number of nodepools is 1")
      case n if n > 200 => Left("Maximum number of nodepools is 200")
      case _            => Right(NumNodepools.apply(n))
    }
  )

  implicit val updateAppRequestDecoder: Decoder[UpdateAppRequest] =
    Decoder.instance { x =>
      for {
        enabled <- x.downField("autodeleteEnabled").as[Option[Boolean]]
        threshold <- x.downField("autodeleteThreshold").as[Option[AutodeleteThreshold]]
      } yield UpdateAppRequest(enabled, threshold)
    }

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
        bucketNameToMount <- x.downField("bucketNameToMount").as[Option[GcsBucketName]]

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
        bucketNameToMount
      )
    }

  implicit val nameKeyEncoder: KeyEncoder[ServiceName] = KeyEncoder.encodeKeyString.contramap(_.value)

  implicit val listAppResponseEncoder: Encoder[ListAppResponse] =
    Encoder.forProduct17(
      "workspaceId",
      "cloudContext",
      "region",
      "kubernetesRuntimeConfig",
      "autopilot",
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
        x.autopilot,
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
    Encoder.forProduct18(
      "workspaceId",
      "appName",
      "cloudContext",
      "region",
      "kubernetesRuntimeConfig",
      "autopilot",
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
