package org.broadinstitute.dsde.workbench.leonardo
package http
package api

import java.util.UUID

import akka.http.scaladsl.marshalling.ToResponseMarshallable
import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server
import JsonCodec._
import akka.http.scaladsl.server.Directives.pathEndOrSingleSlash
import cats.effect.{IO, Timer}
import cats.mtl.ApplicativeAsk
import org.broadinstitute.dsde.workbench.leonardo.api.CookieSupport
import org.broadinstitute.dsde.workbench.model.{TraceId, UserInfo}
import org.broadinstitute.dsde.workbench.model.google.GoogleProject
import akka.http.scaladsl.server.Directives._
import io.circe.{Decoder, Encoder, KeyDecoder, KeyEncoder}
import org.broadinstitute.dsde.workbench.leonardo.http.service.{CreateAppRequest, GetAppResponse, ListAppResponse}
import org.broadinstitute.dsde.workbench.leonardo.http.api.KubernetesRoutes._
import de.heikoseeberger.akkahttpcirce.ErrorAccumulatingCirceSupport._
import RuntimeRoutes._
import org.broadinstitute.dsde.workbench.google2.KubernetesSerializableName.ServiceName
import org.broadinstitute.dsde.workbench.leonardo.service.KubernetesService

class KubernetesRoutes(kubernetesService: KubernetesService[IO], userInfoDirectives: UserInfoDirectives)(
  implicit timer: Timer[IO]
) {

  val routes: server.Route = userInfoDirectives.requireUserInfo { userInfo =>
    CookieSupport.setTokenCookie(userInfo, CookieSupport.tokenCookieName) {
      implicit val traceId = ApplicativeAsk.const[IO, TraceId](TraceId(UUID.randomUUID()))
      pathPrefix("google" / "v1" / "app") {
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
                RouteValidation.validateKubernetesName(appNameString, AppName.apply) { appName =>
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
                        complete(
                          deleteAppHandler(
                            userInfo,
                            googleProject,
                            appName
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
                                    appName: AppName): IO[ToResponseMarshallable] =
    for {
      context <- AppContext.generate[IO]()
      implicit0(ctx: ApplicativeAsk[IO, AppContext]) = ApplicativeAsk.const[IO, AppContext](
        context
      )
      _ <- kubernetesService.deleteApp(
        userInfo,
        googleProject,
        appName
      )
    } yield StatusCodes.Accepted

}

object KubernetesRoutes {
  implicit val createAppDecoder: Decoder[CreateAppRequest] =
    Decoder.forProduct4("kubernetesRuntimeConfig", "appType", "diskConfig", "labels")(CreateAppRequest.apply)
  implicit val nameKeyDecoder: KeyDecoder[ServiceName] = KeyDecoder.decodeKeyString.map(ServiceName.apply)
  implicit val getAppDecoder: Decoder[GetAppResponse] =
    Decoder.forProduct5("kubernetesRuntimeConfig", "errors", "status", "proxyUrls", "diskName")(GetAppResponse.apply)
  implicit val listAppDecoder: Decoder[ListAppResponse] = Decoder.forProduct7("googleProject",
                                                                              "kubernetesRuntimeConfig",
                                                                              "errors",
                                                                              "status",
                                                                              "proxyUrls",
                                                                              "appName",
                                                                              "diskName")(ListAppResponse.apply)

  implicit val createAppEncoder: Encoder[CreateAppRequest] =
    Encoder.forProduct4("kubernetesRuntimeConfig", "appType", "diskConfig", "labels")(x =>
      CreateAppRequest.unapply(x).get
    )

  implicit val nameKeyEncoder: KeyEncoder[ServiceName] = KeyEncoder.encodeKeyString.contramap(_.value)
  implicit val listAppResponseEncoder: Encoder[ListAppResponse] =
    Encoder.forProduct7("googleProject",
                        "kubernetesRuntimeConfig",
                        "errors",
                        "status",
                        "proxyUrls",
                        "appName",
                        "diskName")(x => ListAppResponse.unapply(x).get)

  implicit val getAppResponseEncoder: Encoder[GetAppResponse] =
    Encoder.forProduct5("kubernetesRuntimeConfig", "errors", "status", "proxyUrls", "diskName")(x =>
      GetAppResponse.unapply(x).get
    )
}
