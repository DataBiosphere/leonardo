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
import org.broadinstitute.dsde.workbench.leonardo.JsonCodec.autodeleteThresholdDecoder
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
}
