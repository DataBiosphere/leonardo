package org.broadinstitute.dsde.workbench.leonardo
package http
package api

import akka.http.scaladsl.marshalling.ToResponseMarshallable
import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server
import JsonCodec._
import akka.http.scaladsl.server.Directives.pathEndOrSingleSlash
import cats.effect.{IO, Timer}
import cats.mtl.ApplicativeAsk
import org.broadinstitute.dsde.workbench.leonardo.api.CookieSupport
import org.broadinstitute.dsde.workbench.model.UserInfo
import org.broadinstitute.dsde.workbench.model.google.GoogleProject
import akka.http.scaladsl.server.Directives._
import io.circe.{Decoder, Encoder, KeyEncoder}
import org.broadinstitute.dsde.workbench.leonardo.http.api.AppRoutes._
import de.heikoseeberger.akkahttpcirce.ErrorAccumulatingCirceSupport._
import org.broadinstitute.dsde.workbench.google2.KubernetesSerializableName.ServiceName
import org.broadinstitute.dsde.workbench.leonardo.service.KubernetesService

class AppRoutes(kubernetesService: KubernetesService[IO], userInfoDirectives: UserInfoDirectives)(
  implicit timer: Timer[IO]
) {

  val routes: server.Route = userInfoDirectives.requireUserInfo { userInfo =>
    CookieSupport.setTokenCookie(userInfo, CookieSupport.tokenCookieName) {
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
              path("batchNodepoolCreate") {
                pathEndOrSingleSlash {
                  post {
                    entity(as[BatchNodepoolCreateRequest]) { req =>
                      complete(
                        batchNodepoolCreateHandler(userInfo, googleProject, req)
                      )
                    }
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
                  }
                }
              }
          }
      }
    }
  }

  private[api] def batchNodepoolCreateHandler(userInfo: UserInfo,
                                              googleProject: GoogleProject,
                                              req: BatchNodepoolCreateRequest): IO[ToResponseMarshallable] =
    for {
      context <- AppContext.generate[IO]()
      implicit0(ctx: ApplicativeAsk[IO, AppContext]) = ApplicativeAsk.const[IO, AppContext](
        context
      )
      _ <- kubernetesService.batchNodepoolCreate(userInfo, googleProject, req)
    } yield StatusCodes.Accepted

  private[api] def createAppHandler(userInfo: UserInfo,
                                    googleProject: GoogleProject,
                                    appName: AppName,
                                    req: CreateAppRequest): IO[ToResponseMarshallable] =
    for {
      context <- AppContext.generate[IO]()
      implicit0(ctx: ApplicativeAsk[IO, AppContext]) = ApplicativeAsk.const[IO, AppContext](
        context
      )
      _ <- kubernetesService.createApp(
        userInfo,
        googleProject,
        appName,
        req
      )
    } yield StatusCodes.Accepted

  private[api] def getAppHandler(userInfo: UserInfo,
                                 googleProject: GoogleProject,
                                 appName: AppName): IO[ToResponseMarshallable] =
    for {
      context <- AppContext.generate[IO]()
      implicit0(ctx: ApplicativeAsk[IO, AppContext]) = ApplicativeAsk.const[IO, AppContext](
        context
      )
      resp <- kubernetesService.getApp(
        userInfo,
        googleProject,
        appName
      )
    } yield StatusCodes.OK -> resp

  private[api] def listAppHandler(userInfo: UserInfo,
                                  googleProject: Option[GoogleProject],
                                  params: Map[String, String]): IO[ToResponseMarshallable] =
    for {
      context <- AppContext.generate[IO]()
      implicit0(ctx: ApplicativeAsk[IO, AppContext]) = ApplicativeAsk.const[IO, AppContext](
        context
      )
      resp <- kubernetesService.listApp(
        userInfo,
        googleProject,
        params
      )
    } yield StatusCodes.OK -> resp

  private[api] def deleteAppHandler(userInfo: UserInfo,
                                    googleProject: GoogleProject,
                                    appName: AppName,
                                    params: Map[String, String]): IO[ToResponseMarshallable] =
    for {
      context <- AppContext.generate[IO]()
      implicit0(ctx: ApplicativeAsk[IO, AppContext]) = ApplicativeAsk.const[IO, AppContext](
        context
      )
      deleteDisk = params
        .get("deleteDisk")
        .map(s => s == "true")
        .getOrElse(false) //if `deleteDisk` is explicitly set to true, then we delete disk; otherwise, we don't
      deleteParams = DeleteAppRequest(
        userInfo,
        googleProject,
        appName,
        deleteDisk
      )
      _ <- kubernetesService.deleteApp(
        deleteParams
      )
    } yield StatusCodes.Accepted

}

object AppRoutes {

  implicit val createAppDecoder: Decoder[CreateAppRequest] =
    Decoder.instance { x =>
      for {
        c <- x.downField("kubernetesRuntimeConfig").as[Option[KubernetesRuntimeConfig]]
        a <- x.downField("appType").as[Option[AppType]]
        d <- x.downField("diskConfig").as[Option[PersistentDiskRequest]]
        l <- x.downField("labels").as[Option[LabelMap]]
        cv <- x.downField("customEnvironmentVariables").as[Option[LabelMap]]
      } yield CreateAppRequest(c, a.getOrElse(AppType.Galaxy), d, l.getOrElse(Map.empty), cv.getOrElse(Map.empty))
    }

  implicit val numNodepoolsDecoder: Decoder[NumNodepools] = Decoder.decodeInt.emap(n =>
    n match {
      case n if n < 1   => Left("Minimum number of nodepools is 1")
      case n if n > 200 => Left("Maximum number of nodepools is 200")
      case _            => Right(NumNodepools.apply(n))
    }
  )

  implicit val batchNodepoolCreateRequestDecoder: Decoder[BatchNodepoolCreateRequest] =
    Decoder.forProduct3("numNodepools", "kubernetesRuntimeConfig", "clusterName")(BatchNodepoolCreateRequest.apply)

  implicit val nameKeyEncoder: KeyEncoder[ServiceName] = KeyEncoder.encodeKeyString.contramap(_.value)
  implicit val listAppResponseEncoder: Encoder[ListAppResponse] =
    Encoder.forProduct8("googleProject",
                        "kubernetesRuntimeConfig",
                        "errors",
                        "status",
                        "proxyUrls",
                        "appName",
                        "diskName",
                        "auditInfo")(x => ListAppResponse.unapply(x).get)

  implicit val getAppResponseEncoder: Encoder[GetAppResponse] =
    Encoder.forProduct7("kubernetesRuntimeConfig",
                        "errors",
                        "status",
                        "proxyUrls",
                        "diskName",
                        "customEnvironmentVariables",
                        "auditInfo")(x => GetAppResponse.unapply(x).get)
}
