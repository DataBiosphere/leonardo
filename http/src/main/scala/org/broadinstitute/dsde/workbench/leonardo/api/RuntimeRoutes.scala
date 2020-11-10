package org.broadinstitute.dsde.workbench.leonardo
package http
package api

import java.net.URL

import akka.http.scaladsl.marshalling.ToResponseMarshallable
import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server
import akka.http.scaladsl.server.Directives._
import cats.effect.IO
import cats.implicits._
import cats.mtl.Ask
import de.heikoseeberger.akkahttpcirce.ErrorAccumulatingCirceSupport._
import io.circe.{Decoder, DecodingFailure, Encoder}
import io.opencensus.scala.akka.http.TracingDirective.traceRequestForService
import org.broadinstitute.dsde.workbench.google2.MachineTypeName
import org.broadinstitute.dsde.workbench.leonardo.JsonCodec._
import org.broadinstitute.dsde.workbench.leonardo.api.CookieSupport
import org.broadinstitute.dsde.workbench.leonardo.http.api.LeoRoutesJsonCodec.dataprocConfigDecoder
import org.broadinstitute.dsde.workbench.leonardo.http.api.RuntimeRoutes._
import org.broadinstitute.dsde.workbench.leonardo.http.service.{
  DeleteRuntimeRequest,
  GetRuntimeResponse,
  RuntimeService
}
import org.broadinstitute.dsde.workbench.model.google.GoogleProject
import org.broadinstitute.dsde.workbench.model.UserInfo
import org.broadinstitute.dsde.workbench.openTelemetry.OpenTelemetryMetrics

import scala.concurrent.duration._

class RuntimeRoutes(runtimeService: RuntimeService[IO], userInfoDirectives: UserInfoDirectives)(
  implicit metrics: OpenTelemetryMetrics[IO]
) {
  val routes: server.Route = traceRequestForService(serviceData) { span =>
    extractAppContext(Some(span)) { implicit ctx =>
      userInfoDirectives.requireUserInfo { userInfo =>
        CookieSupport.setTokenCookie(userInfo, CookieSupport.tokenCookieName) {
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
                    RouteValidation.validateNameDirective(runtimeNameString, RuntimeName.apply) { runtimeName =>
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
                        } ~ get {
                          complete(
                            getRuntimeHandler(
                              userInfo,
                              googleProject,
                              runtimeName
                            )
                          )
                        } ~ patch {

                          entity(as[UpdateRuntimeRequest]) { req =>
                            complete(
                              updateRuntimeHandler(
                                userInfo,
                                googleProject,
                                runtimeName,
                                req
                              )
                            )
                          }

                        } ~ delete {
                          parameterMap { params =>
                            complete(
                              deleteRuntimeHandler(
                                userInfo,
                                googleProject,
                                runtimeName,
                                params
                              )
                            )

                          }
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
    }
  }

  private[api] def createRuntimeHandler(userInfo: UserInfo,
                                        googleProject: GoogleProject,
                                        runtimeName: RuntimeName,
                                        req: CreateRuntime2Request)(
    implicit ev: Ask[IO, AppContext]
  ): IO[ToResponseMarshallable] =
    for {
      ctx <- ev.ask[AppContext]
      apiCall = runtimeService.createRuntime(userInfo, googleProject, runtimeName, req)
      _ <- metrics.incrementCounter("createRuntime")
      _ <- ctx.span.fold(apiCall)(span =>
        spanResource[IO](span, "createRuntime")
          .use(_ => apiCall)
      )
    } yield StatusCodes.Accepted: ToResponseMarshallable

  private[api] def getRuntimeHandler(userInfo: UserInfo, googleProject: GoogleProject, runtimeName: RuntimeName)(
    implicit ev: Ask[IO, AppContext]
  ): IO[ToResponseMarshallable] =
    for {
      ctx <- ev.ask[AppContext]
      apiCall = runtimeService.getRuntime(userInfo, googleProject, runtimeName)
      _ <- metrics.incrementCounter("getRuntime")
      resp <- ctx.span.fold(apiCall)(span =>
        spanResource[IO](span, "getRuntime")
          .use(_ => apiCall)
      )
    } yield StatusCodes.OK -> resp: ToResponseMarshallable

  private[api] def listRuntimesHandler(userInfo: UserInfo,
                                       googleProject: Option[GoogleProject],
                                       params: Map[String, String])(
    implicit ev: Ask[IO, AppContext]
  ): IO[ToResponseMarshallable] =
    for {
      ctx <- ev.ask[AppContext]
      apiCall = runtimeService.listRuntimes(userInfo, googleProject, params)
      _ <- metrics.incrementCounter("listRuntime")
      resp <- ctx.span.fold(apiCall)(span =>
        spanResource[IO](span, "listRuntime")
          .use(_ => apiCall)
      )
    } yield StatusCodes.OK -> resp: ToResponseMarshallable

  private[api] def deleteRuntimeHandler(userInfo: UserInfo,
                                        googleProject: GoogleProject,
                                        runtimeName: RuntimeName,
                                        params: Map[String, String])(
    implicit ev: Ask[IO, AppContext]
  ): IO[ToResponseMarshallable] =
    for {
      ctx <- ev.ask[AppContext]
      deleteDisk = params
        .get("deleteDisk")
        .map(s => if (s == "true") true else false)
        .getOrElse(false) //if `deleteDisk` is explicitly set to true, then we delete disk; otherwise, we don't
      request = DeleteRuntimeRequest(userInfo, googleProject, runtimeName, deleteDisk)
      apiCall = runtimeService.deleteRuntime(request)
      _ <- metrics.incrementCounter("deleteRuntime")
      _ <- ctx.span.fold(apiCall)(span =>
        spanResource[IO](span, "deleteRuntime")
          .use(_ => apiCall)
      )
    } yield StatusCodes.Accepted: ToResponseMarshallable

  private[api] def stopRuntimeHandler(userInfo: UserInfo, googleProject: GoogleProject, runtimeName: RuntimeName)(
    implicit ev: Ask[IO, AppContext]
  ): IO[ToResponseMarshallable] =
    for {
      ctx <- ev.ask[AppContext]
      apiCall = runtimeService.stopRuntime(userInfo, googleProject, runtimeName)
      _ <- metrics.incrementCounter("stopRuntime")
      _ <- ctx.span.fold(apiCall)(span =>
        spanResource[IO](span, "stopRuntime")
          .use(_ => apiCall)
      )
    } yield StatusCodes.Accepted: ToResponseMarshallable

  private[api] def startRuntimeHandler(userInfo: UserInfo, googleProject: GoogleProject, runtimeName: RuntimeName)(
    implicit ev: Ask[IO, AppContext]
  ): IO[ToResponseMarshallable] =
    for {
      ctx <- ev.ask[AppContext]
      apiCall = runtimeService.startRuntime(userInfo, googleProject, runtimeName)
      _ <- metrics.incrementCounter("startRuntime")
      _ <- ctx.span.fold(apiCall)(span =>
        spanResource[IO](span, "startRuntime")
          .use(_ => apiCall)
      )
    } yield StatusCodes.Accepted: ToResponseMarshallable

  private[api] def updateRuntimeHandler(userInfo: UserInfo,
                                        googleProject: GoogleProject,
                                        runtimeName: RuntimeName,
                                        req: UpdateRuntimeRequest)(
    implicit ev: Ask[IO, AppContext]
  ): IO[ToResponseMarshallable] =
    for {
      ctx <- ev.ask[AppContext]
      apiCall = runtimeService.updateRuntime(userInfo, googleProject, runtimeName, req)
      _ <- metrics.incrementCounter("updateRuntime")
      _ <- ctx.span.fold(apiCall)(span =>
        spanResource[IO](span, "updateRuntime")
          .use(_ => apiCall)
      )
    } yield StatusCodes.Accepted: ToResponseMarshallable
}

object RuntimeRoutes {
  implicit val gceWithPdConfigDecoder: Decoder[RuntimeConfigRequest.GceWithPdConfig] = Decoder.instance { x =>
    for {
      machineType <- x.downField("machineType").as[Option[MachineTypeName]]
      pd <- x
        .downField("persistentDisk")
        .as[PersistentDiskRequest]
    } yield RuntimeConfigRequest.GceWithPdConfig(machineType, pd)
  }

  implicit val gceConfigDecoder: Decoder[RuntimeConfigRequest.GceConfig] = Decoder.instance { x =>
    for {
      machineType <- x.downField("machineType").as[Option[MachineTypeName]]
      diskSize <- x
        .downField("diskSize")
        .as[Option[DiskSize]]
    } yield RuntimeConfigRequest.GceConfig(machineType, diskSize)
  }

  implicit val runtimeConfigDecoder: Decoder[RuntimeConfigRequest] = Decoder.instance { x =>
    //For newer version of requests, we use `cloudService` field to distinguish whether user is
    for {
      cloudService <- x.downField("cloudService").as[CloudService]
      r <- cloudService match {
        case CloudService.Dataproc =>
          x.as[RuntimeConfigRequest.DataprocConfig]
        case CloudService.GCE =>
          for {
            machineType <- x.downField("machineType").as[Option[MachineTypeName]]
            pd <- x.downField("persistentDisk").as[Option[PersistentDiskRequest]]
            res <- pd match {
              case Some(p) => RuntimeConfigRequest.GceWithPdConfig(machineType, p).asRight[DecodingFailure]
              case None =>
                x.downField("diskSize")
                  .as[Option[DiskSize]]
                  .map(d => RuntimeConfigRequest.GceConfig(machineType, d))
            }
          } yield res
      }
    } yield r
  }

  implicit val createRuntimeRequestDecoder: Decoder[CreateRuntime2Request] = Decoder.instance { c =>
    for {
      l <- c.downField("labels").as[Option[LabelMap]]
      jus <- c.downField("jupyterUserScriptUri").as[Option[UserScriptPath]]
      jsus <- c.downField("jupyterStartUserScriptUri").as[Option[UserScriptPath]]
      rc <- c.downField("runtimeConfig").as[Option[RuntimeConfigRequest]]
      uje <- c.downField("userJupyterExtensionConfig").as[Option[UserJupyterExtensionConfig]]
      a <- c.downField("autopause").as[Option[Boolean]]
      apt <- c.downField("autopauseThreshold").as[Option[Int]]
      dc <- c.downField("defaultClientId").as[Option[String]]
      tdi <- c.downField("toolDockerImage").as[Option[ContainerImage]]
      wr <- c.downField("welderRegistry").as[Option[ContainerRegistry]]
      wdi <- c.downField("welderDockerImage").as[Option[ContainerImage]]
      s <- c.downField("scopes").as[Option[Set[String]]]
      cv <- c.downField("customEnvironmentVariables").as[Option[LabelMap]]
    } yield CreateRuntime2Request(
      l.getOrElse(Map.empty),
      jus,
      jsus,
      rc,
      uje.flatMap(x => if (x.asLabels.isEmpty) None else Some(x)),
      a,
      apt.map(_.minute),
      dc,
      tdi,
      wr,
      wdi,
      s.getOrElse(Set.empty),
      cv.getOrElse(Map.empty)
    )
  }

  implicit val updateGceConfigDecoder: Decoder[UpdateRuntimeConfigRequest.GceConfig] = Decoder.instance { x =>
    for {
      machineType <- x.downField("machineType").as[Option[MachineTypeName]]
      diskSize <- x
        .downField("diskSize")
        .as[Option[DiskSize]]
    } yield UpdateRuntimeConfigRequest.GceConfig(machineType, diskSize)
  }

  implicit val updateDataprocConfigDecoder: Decoder[UpdateRuntimeConfigRequest.DataprocConfig] = Decoder.instance { x =>
    for {
      masterMachineType <- x.downField("masterMachineType").as[Option[MachineTypeName]]
      diskSize <- x
        .downField("masterDiskSize")
        .as[Option[DiskSize]]
      numWorkers <- x.downField("numberOfWorkers").as[Option[Int]].flatMap {
        case Some(x) if x < 0  => Left(negativeNumberDecodingFailure)
        case Some(x) if x == 1 => Left(oneWorkerSpecifiedDecodingFailure)
        case x                 => Right(x)
      }
      numPreemptibles <- x.downField("numberOfPreemptibleWorkers").as[Option[Int]].flatMap {
        case Some(x) if x < 0 => Left(negativeNumberDecodingFailure)
        case x                => Right(x)
      }
    } yield UpdateRuntimeConfigRequest.DataprocConfig(masterMachineType, diskSize, numWorkers, numPreemptibles)
  }

  implicit val updateRuntimeConfigRequestDecoder: Decoder[UpdateRuntimeConfigRequest] = Decoder.instance { x =>
    for {
      cloudService <- x.downField("cloudService").as[CloudService]
      r <- cloudService match {
        case CloudService.Dataproc =>
          x.as[UpdateRuntimeConfigRequest.DataprocConfig]
        case CloudService.GCE =>
          x.as[UpdateRuntimeConfigRequest.GceConfig]
      }
    } yield r
  }

  implicit val updateRuntimeRequestDecoder: Decoder[UpdateRuntimeRequest] = Decoder.instance { x =>
    for {
      rc <- x.downField("runtimeConfig").as[Option[UpdateRuntimeConfigRequest]]
      as <- x.downField("allowStop").as[Option[Boolean]]
      ap <- x.downField("autopause").as[Option[Boolean]]
      at <- x.downField("autopauseThreshold").as[Option[Int]]
      ul <- x.downField("labelsToUpsert").as[Option[LabelMap]]
      dl <- x.downField("labelsToDelete").as[Option[List[String]]]
    } yield {
      UpdateRuntimeRequest(rc,
                           as.getOrElse(false),
                           ap,
                           at.map(_.minutes),
                           ul.getOrElse(Map.empty),
                           dl.getOrElse(List.empty))
    }
  }

  implicit val runtimeStatusEncoder: Encoder[RuntimeStatus] = Encoder.encodeString.contramap { x =>
    x match {
      case RuntimeStatus.PreCreating => RuntimeStatus.Creating.toString
      case RuntimeStatus.PreStarting => RuntimeStatus.Starting.toString
      case RuntimeStatus.PreStopping => RuntimeStatus.Stopping.toString
      case RuntimeStatus.PreDeleting => RuntimeStatus.Deleting.toString
      case _                         => x.toString
    }
  }

  implicit val diskConfigEncoder: Encoder[DiskConfig] = Encoder.forProduct4(
    "name",
    "size",
    "diskType",
    "blockSize"
  )(x => (x.name, x.size, x.diskType, x.blockSize))

  // we're reusing same `GetRuntimeResponse` in LeonardoService.scala as well, but we don't want to encode this object the same way the legacy
  // API does
  implicit val getRuntimeResponseEncoder: Encoder[GetRuntimeResponse] = Encoder.forProduct21(
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
    "jupyterUserScriptUri",
    "jupyterStartUserScriptUri",
    "errors",
    "userJupyterExtensionConfig",
    "autopauseThreshold",
    "defaultClientId",
    "runtimeImages",
    "scopes",
    "customEnvironmentVariables",
    "diskConfig",
    "patchInProgress"
  )(x =>
    (
      x.id,
      x.clusterName,
      x.googleProject,
      x.serviceAccountInfo,
      x.asyncRuntimeFields,
      x.auditInfo,
      x.runtimeConfig,
      x.clusterUrl,
      x.status,
      x.labels,
      x.jupyterUserScriptUri,
      x.jupyterStartUserScriptUri,
      x.errors,
      x.userJupyterExtensionConfig,
      x.autopauseThreshold,
      x.defaultClientId,
      x.clusterImages,
      x.scopes,
      x.customClusterEnvironmentVariables,
      x.diskConfig,
      x.patchInProgress
    )
  )

  // we're reusing same `GetRuntimeResponse` in LeonardoService.scala as well, but we don't want to encode this object the same way the legacy
  // API does
  implicit val listRuntimeResponseEncoder: Encoder[ListRuntimeResponse2] = Encoder.forProduct9(
    "id",
    "runtimeName",
    "googleProject",
    "auditInfo",
    "runtimeConfig",
    "proxyUrl",
    "status",
    "labels",
    "patchInProgress"
  )(x =>
    (
      x.id,
      x.clusterName,
      x.googleProject,
      x.auditInfo,
      x.runtimeConfig,
      x.proxyUrl,
      x.status,
      x.labels,
      x.patchInProgress
    )
  )

}

final case class ListRuntimeResponse2(id: Long,
                                      samResource: RuntimeSamResourceId,
                                      clusterName: RuntimeName,
                                      googleProject: GoogleProject,
                                      auditInfo: AuditInfo,
                                      runtimeConfig: RuntimeConfig,
                                      proxyUrl: URL,
                                      status: RuntimeStatus,
                                      labels: LabelMap,
                                      patchInProgress: Boolean)
