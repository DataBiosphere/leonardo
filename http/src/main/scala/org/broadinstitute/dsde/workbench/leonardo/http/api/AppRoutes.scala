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
import org.broadinstitute.dsde.workbench.leonardo.http.api.AppRoutes.updateAppRequestDecoder
import org.broadinstitute.dsde.workbench.leonardo.http.api.AppV2Routes.{
  createAppDecoder,
  getAppResponseEncoder,
  listAppResponseEncoder
}
import org.broadinstitute.dsde.workbench.leonardo.http.service.AppService
import org.broadinstitute.dsde.workbench.model.UserInfo
import org.broadinstitute.dsde.workbench.model.google.GoogleProject
import org.broadinstitute.dsde.workbench.openTelemetry.OpenTelemetryMetrics

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

  private[api] def updateAppHandler(userInfo: UserInfo,
                                    googleProject: GoogleProject,
                                    appName: AppName,
                                    req: UpdateAppRequest
  )(implicit ev: Ask[IO, AppContext]): IO[ToResponseMarshallable] =
    for {
      ctx <- ev.ask[AppContext]
      apiCall = kubernetesService.updateApp(
        userInfo,
        CloudContext.Gcp(googleProject),
        appName,
        req
      )
      _ <- ctx.span.fold(apiCall)(span => spanResource[IO](span, "updateApp").use(_ => apiCall))
    } yield StatusCodes.Accepted

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
  implicit val numNodepoolsDecoder: Decoder[NumNodepools] = Decoder.decodeInt.emap(n =>
    n match {
      case n if n < 1   => Left("Minimum number of nodepools is 1")
      case n if n > 200 => Left("Maximum number of nodepools is 200")
      case _            => Right(NumNodepools.apply(n))
    }
  )

  implicit val autodeleteThresholdDecoder: Decoder[AutodeleteThreshold] = Decoder.decodeInt.emap {
    case n if n <= 0 => Left("autodeleteThreshold must be a positive number of minutes")
    case n           => Right(AutodeleteThreshold.apply(n))
  }
  implicit val autodeleteThresholdEncoder: Encoder[AutodeleteThreshold] = Encoder.encodeInt.contramap(_.value)

  implicit val updateAppRequestDecoder: Decoder[UpdateAppRequest] =
    Decoder.instance { x =>
      for {
        enabled <- x.downField("autodeleteEnabled").as[Option[Boolean]]
        threshold <- x.downField("autodeleteThreshold").as[Option[AutodeleteThreshold]]
      } yield UpdateAppRequest(enabled, threshold)
    }
}
