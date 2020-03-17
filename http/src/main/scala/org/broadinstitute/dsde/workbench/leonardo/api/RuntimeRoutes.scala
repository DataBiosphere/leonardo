package org.broadinstitute.dsde.workbench.leonardo
package http
package api

import _root_.java.time.Instant
import java.util.UUID

import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
import akka.http.scaladsl.marshalling.ToResponseMarshallable
import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server
import akka.http.scaladsl.server.Directives._
import cats.effect.{IO, Timer}
import cats.mtl.ApplicativeAsk
import de.heikoseeberger.akkahttpcirce.ErrorAccumulatingCirceSupport._
import io.circe.Decoder
import org.broadinstitute.dsde.workbench.leonardo.JsonCodec._
import org.broadinstitute.dsde.workbench.leonardo.api.CookieSupport
import org.broadinstitute.dsde.workbench.leonardo.http.api.LeoRoutes.validateRuntimeNameDirective
import org.broadinstitute.dsde.workbench.leonardo.http.api.LeoRoutesJsonCodec.dataprocConfigDecoder
import org.broadinstitute.dsde.workbench.leonardo.http.api.LeoRoutesSprayJsonCodec._
import org.broadinstitute.dsde.workbench.leonardo.http.api.RuntimeRoutes._
import org.broadinstitute.dsde.workbench.leonardo.http.service.{RuntimeConfigRequest, RuntimeService}
import org.broadinstitute.dsde.workbench.model.google.{GcsPath, GoogleProject}
import org.broadinstitute.dsde.workbench.model.{TraceId, UserInfo}

import scala.concurrent.duration._

class RuntimeRoutes(runtimeService: RuntimeService[IO], userInfoDirectives: UserInfoDirectives)(
  implicit timer: Timer[IO]
) {
  val routes: server.Route = userInfoDirectives.requireUserInfo { userInfo =>
    CookieSupport.setTokenCookie(userInfo, CookieSupport.tokenCookieName) {
      implicit val traceId = ApplicativeAsk.const[IO, TraceId](TraceId(UUID.randomUUID()))
      pathPrefix("google" / "v1") {
        pathPrefix("runtime") {
          pathPrefix(googleProjectSegment / Segment) { (googleProject, runtimeNameString) =>
            validateRuntimeNameDirective(runtimeNameString) { runtimeName =>
              pathEndOrSingleSlash {
                post {
                  entity(as[CreateRuntime2Request]) { req =>
                    complete(
                      createRuntimeHandler(
                        userInfo,
                        googleProject,
                        runtimeName,
                        req
                      )
                    )
                  }
                } ~
                  get {
                    complete(
                      getRuntimeHandler(
                        userInfo,
                        googleProject,
                        runtimeName
                      )
                    )
                  } ~
                  patch {
                    // TODO
                    complete(StatusCodes.NotImplemented)
                  } ~
                  delete {
                    complete(
                      deleteRuntimeHandler(
                        userInfo,
                        googleProject,
                        runtimeName
                      )
                    )
                  }
              } ~
                path("stop") {
                  post {
                    complete(
                      stopRuntimeHandler(
                        userInfo,
                        googleProject,
                        runtimeName
                      )
                    )
                  }
                } ~
                path("start") {
                  post {
                    complete(
                      startRuntimeHandler(
                        userInfo,
                        googleProject,
                        runtimeName
                      )
                    )
                  }
                }
            }
          }
        } ~
          pathPrefix("runtimes") {
            parameterMap { params =>
              path(googleProjectSegment) { googleProject =>
                get {
                  complete(
                    listRuntimesHandler(
                      userInfo,
                      Some(googleProject),
                      params
                    )
                  )
                }
              } ~
                pathEndOrSingleSlash {
                  get {
                    complete(
                      listRuntimesHandler(
                        userInfo,
                        None,
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

  private[api] def createRuntimeHandler(userInfo: UserInfo,
                                        googleProject: GoogleProject,
                                        runtimeName: RuntimeName,
                                        req: CreateRuntime2Request): IO[ToResponseMarshallable] =
    for {
      context <- RuntimeServiceContext.generate
      implicit0(ctx: ApplicativeAsk[IO, RuntimeServiceContext]) = ApplicativeAsk.const[IO, RuntimeServiceContext](
        context
      )
      _ <- runtimeService.createRuntime(
        userInfo,
        googleProject,
        runtimeName,
        req
      )
    } yield StatusCodes.Accepted

  private[api] def getRuntimeHandler(userInfo: UserInfo,
                                     googleProject: GoogleProject,
                                     runtimeName: RuntimeName): IO[ToResponseMarshallable] =
    for {
      context <- RuntimeServiceContext.generate
      implicit0(ctx: ApplicativeAsk[IO, RuntimeServiceContext]) = ApplicativeAsk.const[IO, RuntimeServiceContext](
        context
      )
      resp <- runtimeService.getRuntime(userInfo, googleProject, runtimeName)
    } yield StatusCodes.OK -> resp

  private[api] def listRuntimesHandler(userInfo: UserInfo,
                                       googleProject: Option[GoogleProject],
                                       params: Map[String, String]): IO[ToResponseMarshallable] =
    for {
      context <- RuntimeServiceContext.generate
      implicit0(ctx: ApplicativeAsk[IO, RuntimeServiceContext]) = ApplicativeAsk.const[IO, RuntimeServiceContext](
        context
      )
      resp <- runtimeService.listRuntimes(userInfo, googleProject, params)
    } yield StatusCodes.OK -> resp

  private[api] def deleteRuntimeHandler(userInfo: UserInfo,
                                        googleProject: GoogleProject,
                                        runtimeName: RuntimeName): IO[ToResponseMarshallable] =
    for {
      context <- RuntimeServiceContext.generate
      implicit0(ctx: ApplicativeAsk[IO, RuntimeServiceContext]) = ApplicativeAsk.const[IO, RuntimeServiceContext](
        context
      )
      _ <- runtimeService.deleteRuntime(userInfo, googleProject, runtimeName)
    } yield StatusCodes.Accepted

  private[api] def stopRuntimeHandler(userInfo: UserInfo,
                                      googleProject: GoogleProject,
                                      runtimeName: RuntimeName): IO[ToResponseMarshallable] =
    for {
      context <- RuntimeServiceContext.generate
      implicit0(ctx: ApplicativeAsk[IO, RuntimeServiceContext]) = ApplicativeAsk.const[IO, RuntimeServiceContext](
        context
      )
      _ <- runtimeService.stopRuntime(userInfo, googleProject, runtimeName)
    } yield StatusCodes.Accepted

  private[api] def startRuntimeHandler(userInfo: UserInfo,
                                       googleProject: GoogleProject,
                                       runtimeName: RuntimeName): IO[ToResponseMarshallable] =
    for {
      context <- RuntimeServiceContext.generate
      implicit0(ctx: ApplicativeAsk[IO, RuntimeServiceContext]) = ApplicativeAsk.const[IO, RuntimeServiceContext](
        context
      )
      _ <- runtimeService.startRuntime(userInfo, googleProject, runtimeName)
    } yield StatusCodes.Accepted
}

object RuntimeRoutes {
  implicit val gceConfigDecoder: Decoder[RuntimeConfigRequest.GceConfig] = Decoder.forProduct2(
    "machineType",
    "diskSize"
  )((mt, ds) => RuntimeConfigRequest.GceConfig(mt, ds))

  implicit val runtimeConfigDecoder: Decoder[RuntimeConfigRequest] = Decoder.instance { x =>
    //For newer version of requests, we use `cloudService` field to distinguish whether user is
    for {
      cloudService <- x.downField("cloudService").as[CloudService]
      r <- cloudService match {
        case CloudService.Dataproc =>
          x.as[RuntimeConfigRequest.DataprocConfig]
        case CloudService.GCE =>
          x.as[RuntimeConfigRequest.GceConfig]
      }
    } yield r
  }

  implicit val createRuntimeRequestDecoder: Decoder[CreateRuntime2Request] = Decoder.instance { c =>
    for {
      l <- c.downField("labels").as[Option[LabelMap]]
      je <- c.downField("jupyterExtensionUri").as[Option[GcsPath]]
      jus <- c.downField("jupyterUserScriptUri").as[Option[UserScriptPath]]
      jsus <- c.downField("jupyterStartUserScriptUri").as[Option[UserScriptPath]]
      rc <- c.downField("runtimeConfig").as[Option[RuntimeConfigRequest]]
      uje <- c.downField("userJupyterExtensionConfig").as[Option[UserJupyterExtensionConfig]]
      a <- c.downField("autopause").as[Option[Boolean]]
      apt <- c.downField("autopauseThreshold").as[Option[Int]]
      dc <- c.downField("defaultClientId").as[Option[String]]
      tdi <- c.downField("toolDockerImage").as[Option[ContainerImage]]
      wdi <- c.downField("welderDockerImage").as[Option[ContainerImage]]
      s <- c.downField("scopes").as[Option[Set[String]]]
      c <- c.downField("customEnvironmentVariables").as[Option[LabelMap]]
    } yield CreateRuntime2Request(
      l.getOrElse(Map.empty),
      je,
      jus,
      jsus,
      rc,
      uje,
      a,
      apt.map(_.minute),
      dc,
      tdi,
      wdi,
      s.getOrElse(Set.empty),
      c.getOrElse(Map.empty)
    )
  }
}

final case class RuntimeServiceContext(traceId: TraceId, now: Instant)
object RuntimeServiceContext {
  def generate(implicit timer: Timer[IO]): IO[RuntimeServiceContext] =
    for {
      traceId <- IO(UUID.randomUUID())
      now <- nowInstant[IO]
    } yield RuntimeServiceContext(TraceId(traceId), now)
}

final case class CreateRuntime2Request(labels: LabelMap,
                                       jupyterExtensionUri: Option[GcsPath],
                                       jupyterUserScriptUri: Option[UserScriptPath],
                                       jupyterStartUserScriptUri: Option[UserScriptPath],
                                       runtimeConfig: Option[RuntimeConfigRequest],
                                       userJupyterExtensionConfig: Option[UserJupyterExtensionConfig],
                                       autopause: Option[Boolean],
                                       autopauseThreshold: Option[FiniteDuration],
                                       defaultClientId: Option[String],
                                       toolDockerImage: Option[ContainerImage],
                                       welderDockerImage: Option[ContainerImage],
                                       scopes: Set[String],
                                       customEnvironmentVariables: Map[String, String])
