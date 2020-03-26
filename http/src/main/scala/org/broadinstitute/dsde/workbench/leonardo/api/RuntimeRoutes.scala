package org.broadinstitute.dsde.workbench.leonardo
package http
package api

import _root_.java.time.Instant
import java.util.UUID

import akka.http.scaladsl.marshalling.ToResponseMarshallable
import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server
import akka.http.scaladsl.server.Directives._
import cats.effect.{IO, Timer}
import cats.mtl.ApplicativeAsk
import de.heikoseeberger.akkahttpcirce.ErrorAccumulatingCirceSupport._
import io.circe.{Decoder, Encoder}
import org.broadinstitute.dsde.workbench.google2.MachineTypeName
import org.broadinstitute.dsde.workbench.leonardo.JsonCodec._
import org.broadinstitute.dsde.workbench.leonardo.api.CookieSupport
import org.broadinstitute.dsde.workbench.leonardo.http.api.LeoRoutes.validateRuntimeNameDirective
import org.broadinstitute.dsde.workbench.leonardo.http.api.LeoRoutesJsonCodec.dataprocConfigDecoder
import org.broadinstitute.dsde.workbench.leonardo.http.api.RuntimeRoutes._
import org.broadinstitute.dsde.workbench.leonardo.http.service.{GetRuntimeResponse, ListRuntimeResponse, RuntimeConfigRequest, RuntimeService}
import org.broadinstitute.dsde.workbench.model.google.{GcsPath, GoogleProject}
import org.broadinstitute.dsde.workbench.model.{TraceId, UserInfo}

import scala.concurrent.duration._

class RuntimeRoutes(runtimeService: RuntimeService[IO], userInfoDirectives: UserInfoDirectives)(
  implicit timer: Timer[IO]
) {
  val routes: server.Route = userInfoDirectives.requireUserInfo { userInfo =>
    CookieSupport.setTokenCookie(userInfo, CookieSupport.tokenCookieName) {
      implicit val traceId = ApplicativeAsk.const[IO, TraceId](TraceId(UUID.randomUUID()))
      pathPrefix("google" / "v1" / "runtimes") {
        pathEndOrSingleSlash {
          parameterMap { params =>
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
        } ~
          pathPrefix(googleProjectSegment) { googleProject =>
            pathEndOrSingleSlash {
              parameterMap { params =>
                get {
                  complete(
                    listRuntimesHandler(
                      userInfo,
                      Some(googleProject),
                      params
                    )
                  )
                }
              }
            } ~
              pathPrefix(Segment) { runtimeNameString =>
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

  private[api] def updateRuntimeHandler(userInfo: UserInfo,
                                        googleProject: GoogleProject,
                                        runtimeName: RuntimeName,
                                        req: UpdateRuntimeRequest): IO[ToResponseMarshallable] =
    for {
      context <- RuntimeServiceContext.generate
      implicit0(ctx: ApplicativeAsk[IO, RuntimeServiceContext]) = ApplicativeAsk.const[IO, RuntimeServiceContext](
        context
      )
      _ <- runtimeService.updateRuntime(userInfo, googleProject, runtimeName, req)
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

  // we're reusing same `GetRuntimeResponse` in LeonardoService.scala as well, but we don't want to encode this object the same way the legacy
  // API does
  implicit val getRuntimeResponseEncoder: Encoder[GetRuntimeResponse] = Encoder.forProduct20(
    "id",
    "runtimeName",
    "googleProject",
    "serviceAccount",
    "asyncRuntimeFields",
    "auditInfo",
    "runtimeConfig",
    "proxyUrl",
    "status",
    "labels",
    "jupyterExtensionUri",
    "jupyterUserScriptUri",
    "jupyterStartUserScriptUri",
    "errors",
    "userJupyterExtensionConfig",
    "autopauseThreshold",
    "defaultClientId",
    "runtimeImages",
    "scopes",
    "customEnvironmentVariables"
  )(
    x =>
      (
        x.id,
        x.clusterName,
        x.googleProject,
        x.serviceAccountInfo.clusterServiceAccount.get,
        x.asyncRuntimeFields,
        x.auditInfo,
        x.runtimeConfig,
        x.clusterUrl,
        x.status,
        x.labels,
        x.jupyterExtensionUri,
        x.jupyterUserScriptUri,
        x.jupyterStartUserScriptUri,
        x.errors,
        x.userJupyterExtensionConfig,
        x.autopauseThreshold,
        x.defaultClientId,
        x.clusterImages,
        x.scopes,
        x.customClusterEnvironmentVariables
      )
  )

  // we're reusing same `GetRuntimeResponse` in LeonardoService.scala as well, but we don't want to encode this object the same way the legacy
  // API does
  implicit val listRuntimeResponseEncoder: Encoder[ListRuntimeResponse] = Encoder.forProduct14(
    "id",
    "runtimeName",
    "googleProject",
    "serviceAccount",
    "asyncRuntimeFields",
    "auditInfo",
    "runtimeConfig",
    "proxyUrl",
    "status",
    "labels",
    "jupyterExtensionUri",
    "jupyterUserScriptUri",
    "autopauseThreshold",
    "defaultClientId"
  )(
    x =>
      (
        x.id,
        x.clusterName,
        x.googleProject,
        x.serviceAccountInfo.clusterServiceAccount.get,
        x.asyncRuntimeFields,
        x.auditInfo,
        x.machineConfig,
        x.clusterUrl,
        x.status,
        x.labels,
        x.jupyterExtensionUri,
        x.jupyterUserScriptUri,
        x.autopauseThreshold,
        x.defaultClientId
      )
  )
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

sealed trait UpdateRuntimeConfigRequest extends Product with Serializable {
  def cloudService: CloudService
}
object UpdateRuntimeConfigRequest {
  final case class GceConfig(updatedMachineType: Option[MachineTypeName], updatedDiskSize: Option[Int]) extends UpdateRuntimeConfigRequest {
    val cloudService: CloudService = CloudService.GCE
  }
  final case class DataprocConfig(updatedMasterMachineType: Option[MachineTypeName], updatedMasterDiskSize: Option[Int], updatedNumberOfWorkers: Option[Int], updatedNumberOfPreemptibleWorkers: Option[Int]) extends UpdateRuntimeConfigRequest {
    val cloudService: CloudService = CloudService.Dataproc
  }
}

final case class UpdateRuntimeRequest(updatedRuntimeConfig: Option[UpdateRuntimeConfigRequest],
                                      allowStop: Option[Boolean],
                                      updateAutopauseEnabled: Option[Boolean],
                                      updateAutopauseThreshold: Option[FiniteDuration])