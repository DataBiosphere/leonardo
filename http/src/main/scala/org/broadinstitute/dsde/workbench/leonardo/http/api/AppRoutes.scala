package org.broadinstitute.dsde.workbench.leonardo
package http
package api

import akka.http.scaladsl.marshalling.ToResponseMarshallable
import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server
import akka.http.scaladsl.server.Directives.{pathEndOrSingleSlash, _}
import cats.effect.IO
import cats.mtl.Ask
import de.heikoseeberger.akkahttpcirce.ErrorAccumulatingCirceSupport._
import io.circe.{Decoder, Encoder, KeyEncoder}
import io.opencensus.scala.akka.http.TracingDirective.traceRequestForService
import org.broadinstitute.dsde.workbench.google2.KubernetesSerializableName.ServiceName
import org.broadinstitute.dsde.workbench.leonardo.JsonCodec._
import org.broadinstitute.dsde.workbench.leonardo.http.api.AppRoutes._
import org.broadinstitute.dsde.workbench.leonardo.http.service.AppService
import org.broadinstitute.dsde.workbench.model.UserInfo
import org.broadinstitute.dsde.workbench.model.google.GoogleProject
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
  )(implicit
    ev: Ask[IO, AppContext]
  ): IO[ToResponseMarshallable] =
    for {
      ctx <- ev.ask[AppContext]
      apiCall = kubernetesService.createApp(
        userInfo,
        CloudContext.Gcp(googleProject),
        appName,
        req
      )
      tags = Map("appType" -> req.appType.toString)
      _ <- metrics.incrementCounter("createApp", 1, tags)
      _ <- ctx.span.fold(apiCall)(span => spanResource[IO](span, "createApp").use(_ => apiCall))
    } yield StatusCodes.Accepted

  private[api] def getAppHandler(userInfo: UserInfo, googleProject: GoogleProject, appName: AppName)(implicit
    ev: Ask[IO, AppContext]
  ): IO[ToResponseMarshallable] =
    for {
      ctx <- ev.ask[AppContext]
      apiCall = kubernetesService.getApp(
        userInfo,
        CloudContext.Gcp(googleProject),
        appName
      )
      _ <- metrics.incrementCounter("getApp")
      resp <- ctx.span.fold(apiCall)(span => spanResource[IO](span, "getApp").use(_ => apiCall))
    } yield StatusCodes.OK -> resp

  private[api] def listAppHandler(userInfo: UserInfo,
                                  googleProject: Option[GoogleProject],
                                  params: Map[String, String]
  )(implicit
    ev: Ask[IO, AppContext]
  ): IO[ToResponseMarshallable] =
    for {
      ctx <- ev.ask[AppContext]
      apiCall = kubernetesService.listApp(
        userInfo,
        googleProject.map(CloudContext.Gcp),
        params
      )
      _ <- metrics.incrementCounter("listApp")
      resp <- ctx.span.fold(apiCall)(span => spanResource[IO](span, "listApp").use(_ => apiCall))
    } yield StatusCodes.OK -> resp

  private[api] def deleteAppHandler(userInfo: UserInfo,
                                    googleProject: GoogleProject,
                                    appName: AppName,
                                    params: Map[String, String]
  )(implicit
    ev: Ask[IO, AppContext]
  ): IO[ToResponseMarshallable] =
    for {
      ctx <- ev.ask[AppContext]
      // if `deleteDisk` is explicitly set to true, then we delete disk; otherwise, we don't
      deleteDisk = params.get("deleteDisk").exists(_ == "true")
      apiCall = kubernetesService.deleteApp(
        userInfo,
        CloudContext.Gcp(googleProject),
        appName,
        deleteDisk
      )
      _ <- ctx.span.fold(apiCall)(span => spanResource[IO](span, "deleteApp").use(_ => apiCall))
    } yield StatusCodes.Accepted

  private[api] def stopAppHandler(userInfo: UserInfo, googleProject: GoogleProject, appName: AppName)(implicit
    ev: Ask[IO, AppContext]
  ): IO[ToResponseMarshallable] =
    for {
      ctx <- ev.ask[AppContext]
      apiCall = kubernetesService.stopApp(userInfo, CloudContext.Gcp(googleProject), appName)
      _ <- ctx.span.fold(apiCall)(span => spanResource[IO](span, "stopApp").use(_ => apiCall))
    } yield StatusCodes.Accepted

  private[api] def startAppHandler(userInfo: UserInfo, googleProject: GoogleProject, appName: AppName)(implicit
    ev: Ask[IO, AppContext]
  ): IO[ToResponseMarshallable] =
    for {
      ctx <- ev.ask[AppContext]
      apiCall = kubernetesService.startApp(userInfo, CloudContext.Gcp(googleProject), appName)
      _ <- ctx.span.fold(apiCall)(span => spanResource[IO](span, "startApp").use(_ => apiCall))
    } yield StatusCodes.Accepted
}

object AppRoutes {
  implicit val createAppDecoder: Decoder[CreateAppRequest] =
    Decoder.instance { x =>
      for {
        c <- x.downField("kubernetesRuntimeConfig").as[Option[KubernetesRuntimeConfig]]
        a <- x.downField("appType").as[Option[AppType]]
        s <- x.downField("accessScope").as[Option[AppAccessScope]]
        d <- x.downField("diskConfig").as[Option[PersistentDiskRequest]]
        l <- x.downField("labels").as[Option[LabelMap]]
        cv <- x.downField("customEnvironmentVariables").as[Option[LabelMap]]
        dp <- x.downField("descriptorPath").as[Option[Uri]]
        ea <- x.downField("extraArgs").as[Option[List[String]]]
        swi <- x.downField("sourceWorkspaceId").as[Option[WorkspaceId]]
      } yield CreateAppRequest(c,
                               a.getOrElse(AppType.Galaxy),
                               s,
                               d,
                               l.getOrElse(Map.empty),
                               cv.getOrElse(Map.empty),
                               dp,
                               ea.getOrElse(List.empty),
                               swi
      )
    }

  implicit val numNodepoolsDecoder: Decoder[NumNodepools] = Decoder.decodeInt.emap(n =>
    n match {
      case n if n < 1   => Left("Minimum number of nodepools is 1")
      case n if n > 200 => Left("Maximum number of nodepools is 200")
      case _            => Right(NumNodepools.apply(n))
    }
  )

  implicit val nameKeyEncoder: KeyEncoder[ServiceName] = KeyEncoder.encodeKeyString.contramap(_.value)
  implicit val listAppResponseEncoder: Encoder[ListAppResponse] =
    Encoder.forProduct12(
      "workspaceId",
      "cloudContext",
      "kubernetesRuntimeConfig",
      "errors",
      "status",
      "proxyUrls",
      "appName",
      "appType",
      "diskName",
      "auditInfo",
      "accessScope",
      "labels"
    )(x =>
      (x.workspaceId,
       x.cloudContext,
       x.kubernetesRuntimeConfig,
       x.errors,
       x.status,
       x.proxyUrls,
       x.appName,
       x.appType,
       x.diskName,
       x.auditInfo,
       x.accessScope,
       x.labels
      )
    )

  implicit val getAppResponseEncoder: Encoder[GetAppResponse] =
    Encoder.forProduct12(
      "appName",
      "cloudContext",
      "kubernetesRuntimeConfig",
      "errors",
      "status",
      "proxyUrls",
      "diskName",
      "customEnvironmentVariables",
      "auditInfo",
      "appType",
      "accessScope",
      "labels"
    )(x => GetAppResponse.unapply(x).get)
}
