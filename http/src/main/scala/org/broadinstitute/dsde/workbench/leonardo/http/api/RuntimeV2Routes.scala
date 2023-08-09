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
import org.broadinstitute.dsde.workbench.leonardo.config.RefererConfig
import org.broadinstitute.dsde.workbench.leonardo.http.service.RuntimeV2Service
import org.broadinstitute.dsde.workbench.model.UserInfo
import org.broadinstitute.dsde.workbench.openTelemetry.OpenTelemetryMetrics
import JsonCodec._
import RuntimeRoutesCodec._
import com.azure.resourcemanager.compute.models.VirtualMachineSizeTypes

class RuntimeV2Routes(saturnIframeExtensionHostConfig: RefererConfig,
                      runtimeV2Service: RuntimeV2Service[IO],
                      userInfoDirectives: UserInfoDirectives
)(implicit
  metrics: OpenTelemetryMetrics[IO]
) {
  // See https://github.com/DataBiosphere/terra-ui/blob/ef88f396a61383ee08beb65a37af7cae9476cc20/src/libs/ajax.js#L1358
  private val allValidSaturnIframeExtensions =
    saturnIframeExtensionHostConfig.validHosts.map(s => s"https://${s}/jupyter-iframe-extension.js")

  val routes: server.Route = traceRequestForService(serviceData) { span =>
    extractAppContext(Some(span)) { implicit ctx =>
      userInfoDirectives.requireUserInfo { userInfo =>
        CookieSupport.setTokenCookie(userInfo) {
          pathPrefix("v2" / "runtimes") {
            pathEndOrSingleSlash {
              parameterMap { params =>
                get {
                  complete(
                    listRuntimesHandler(
                      userInfo,
                      None,
                      None,
                      params
                    )
                  )
                }
              }
            } ~
              pathPrefix(workspaceIdSegment) { workspaceId =>
                pathEndOrSingleSlash {
                  parameterMap { params =>
                    get {
                      complete(
                        listRuntimesHandler(
                          userInfo,
                          Some(workspaceId),
                          None,
                          params
                        )
                      )
                    }
                  }
                } ~ pathPrefix("deleteAll") {
                  post {
                    parameterMap { params =>
                      complete(
                        deleteAllRuntimesForWorkspaceHandler(userInfo, workspaceId, params)
                      )
                    }
                  }
                } ~ pathPrefix(runtimeNameSegmentWithValidation) { runtimeName =>
                  path("stop") {
                    post {
                      complete(
                        stopRuntimeHandler(
                          userInfo,
                          workspaceId,
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
                            workspaceId,
                            runtimeName
                          )
                        )
                      }
                    } ~
                    path("updateDateAccessed") {
                      patch {
                        complete(
                          updateDateAccessedHandler(
                            userInfo,
                            workspaceId,
                            runtimeName
                          )
                        )
                      }
                    }
                } ~
                  pathPrefix("azure") {
                    pathEndOrSingleSlash {
                      parameterMap { params =>
                        get {
                          complete(
                            listRuntimesHandler(
                              userInfo,
                              Some(workspaceId),
                              Some(CloudProvider.Azure),
                              params
                            )
                          )
                        }
                      }
                    } ~
                      pathPrefix(runtimeNameSegmentWithValidation) { runtimeName =>
                        pathEndOrSingleSlash {
                          post {
                            parameterMap { params =>
                              entity(as[CreateAzureRuntimeRequest]) { req =>
                                complete(
                                  createAzureRuntimeHandler(userInfo, workspaceId, runtimeName, req, params)
                                )
                              }
                            }
                          } ~ get {
                            complete(
                              getAzureRuntimeHandler(userInfo, workspaceId, runtimeName)
                            )
                          } ~ patch {
                            entity(as[UpdateAzureRuntimeRequest]) { req =>
                              complete(
                                updateAzureRuntimeHandler(userInfo, workspaceId, runtimeName, req)
                              )
                            }
                          } ~ delete {
                            parameterMap { params =>
                              complete(
                                deleteAzureRuntimeHandler(userInfo, workspaceId, runtimeName, params)
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
  }

  private[api] def createAzureRuntimeHandler(userInfo: UserInfo,
                                             workspaceId: WorkspaceId,
                                             runtimeName: RuntimeName,
                                             req: CreateAzureRuntimeRequest,
                                             params: Map[String, String]
  )(implicit
    ev: Ask[IO, AppContext]
  ): IO[ToResponseMarshallable] =
    for {
      ctx <- ev.ask[AppContext]
      // if not specified, create new disk
      useExistingDisk = params.get("useExistingDisk").exists(_ == "true")
      apiCall = runtimeV2Service.createRuntime(userInfo, runtimeName, workspaceId, useExistingDisk, req)
      _ <- metrics.incrementCounter("createRuntimeV2")
      resp <- ctx.span.fold(apiCall)(span =>
        spanResource[IO](span, "createRuntimeV2")
          .use(_ => apiCall)
      )
    } yield StatusCodes.Accepted -> resp: ToResponseMarshallable

  private[api] def getAzureRuntimeHandler(userInfo: UserInfo, workspaceId: WorkspaceId, runtimeName: RuntimeName)(
    implicit ev: Ask[IO, AppContext]
  ): IO[ToResponseMarshallable] =
    for {
      ctx <- ev.ask[AppContext]
      apiCall = runtimeV2Service.getRuntime(userInfo, runtimeName, workspaceId)
      _ <- metrics.incrementCounter("getRuntimeV2")
      resp <- ctx.span.fold(apiCall)(span =>
        spanResource[IO](span, "getRuntimeV2")
          .use(_ => apiCall)
      )
    } yield StatusCodes.OK -> resp: ToResponseMarshallable

  private[api] def startRuntimeHandler(userInfo: UserInfo, workspaceId: WorkspaceId, runtimeName: RuntimeName)(implicit
    ev: Ask[IO, AppContext]
  ): IO[ToResponseMarshallable] =
    for {
      ctx <- ev.ask[AppContext]
      apiCall = runtimeV2Service.startRuntime(userInfo, runtimeName, workspaceId)
      _ <- metrics.incrementCounter("startRuntimeV2")
      resp <- ctx.span.fold(apiCall)(span =>
        spanResource[IO](span, "startRuntimeV2")
          .use(_ => apiCall)
      )
    } yield StatusCodes.Accepted -> resp: ToResponseMarshallable

  private[api] def stopRuntimeHandler(userInfo: UserInfo, workspaceId: WorkspaceId, runtimeName: RuntimeName)(implicit
    ev: Ask[IO, AppContext]
  ): IO[ToResponseMarshallable] =
    for {
      ctx <- ev.ask[AppContext]
      apiCall = runtimeV2Service.stopRuntime(userInfo, runtimeName, workspaceId)
      _ <- metrics.incrementCounter("stopRuntimeV2")
      resp <- ctx.span.fold(apiCall)(span =>
        spanResource[IO](span, "stopRuntimeV2")
          .use(_ => apiCall)
      )
    } yield StatusCodes.Accepted -> resp: ToResponseMarshallable

  def updateAzureRuntimeHandler(userInfo: UserInfo,
                                workspaceId: WorkspaceId,
                                runtimeName: RuntimeName,
                                req: UpdateAzureRuntimeRequest
  )(implicit
    ev: Ask[IO, AppContext]
  ): IO[ToResponseMarshallable] =
    for {
      ctx <- ev.ask[AppContext]
      apiCall = runtimeV2Service.updateRuntime(userInfo, runtimeName, workspaceId, req)
      _ <- metrics.incrementCounter("updateRuntimeV2")
      _ <- ctx.span.fold(apiCall)(span =>
        spanResource[IO](span, "updateRuntimeV2")
          .use(_ => apiCall)
      )
    } yield StatusCodes.Accepted: ToResponseMarshallable

  def deleteAzureRuntimeHandler(userInfo: UserInfo,
                                workspaceId: WorkspaceId,
                                runtimeName: RuntimeName,
                                params: Map[String, String]
  )(implicit
    ev: Ask[IO, AppContext]
  ): IO[ToResponseMarshallable] =
    for {
      ctx <- ev.ask[AppContext]
      // default to true, if `deleteDisk` is explicitly set to false, then we don't delete disk
      deleteDisk = !params.get("deleteDisk").exists(_ == "false")
      apiCall = runtimeV2Service.deleteRuntime(userInfo, runtimeName, workspaceId, deleteDisk)
      tags = Map("deleteDisk" -> deleteDisk.toString)
      _ <- metrics.incrementCounter("deleteRuntimeV2", 1, tags)
      _ <- ctx.span.fold(apiCall)(span =>
        spanResource[IO](span, "deleteRuntimeV2")
          .use(_ => apiCall)
      )
    } yield StatusCodes.Accepted: ToResponseMarshallable

  def deleteAllRuntimesForWorkspaceHandler(userInfo: UserInfo, workspaceId: WorkspaceId, params: Map[String, String])(
    implicit ev: Ask[IO, AppContext]
  ): IO[ToResponseMarshallable] =
    for {
      ctx <- ev.ask[AppContext]
      // default to true, if `deleteDisk` is explicitly set to false, then we don't delete disk
      deleteDisk = !params.get("deleteDisk").exists(_ == "false")
      apiCall = runtimeV2Service.deleteAllRuntimes(userInfo, workspaceId, deleteDisk)
      tags = Map("deleteDisk" -> deleteDisk.toString)
      _ <- metrics.incrementCounter("deleteAllRuntimesV2", 1, tags)
      _ <- ctx.span.fold(apiCall)(span =>
        spanResource[IO](span, "deleteAllRuntimesV2")
          .use(_ => apiCall)
      )
    } yield StatusCodes.Accepted: ToResponseMarshallable

  private[api] def listRuntimesHandler(userInfo: UserInfo,
                                       workspaceId: Option[WorkspaceId],
                                       cloudProvider: Option[CloudProvider],
                                       params: Map[String, String]
  )(implicit
    ev: Ask[IO, AppContext]
  ): IO[ToResponseMarshallable] =
    for {
      ctx <- ev.ask[AppContext]
      apiCall = runtimeV2Service.listRuntimes(userInfo, workspaceId, cloudProvider, params)
      _ <- metrics.incrementCounter("listRuntimeV2")
      resp <- ctx.span.fold(apiCall)(span =>
        spanResource[IO](span, "listRuntimeV2")
          .use(_ => apiCall)
      )
    } yield StatusCodes.OK -> resp: ToResponseMarshallable

  private[api] def updateDateAccessedHandler(userInfo: UserInfo, workspaceId: WorkspaceId, runtimeName: RuntimeName)(
    implicit ev: Ask[IO, AppContext]
  ): IO[ToResponseMarshallable] =
    for {
      ctx <- ev.ask[AppContext]
      apiCall = runtimeV2Service.updateDateAccessed(userInfo, workspaceId, runtimeName)
      _ <- metrics.incrementCounter("updateDateAccessed")
      _ <- ctx.span.fold(apiCall)(span =>
        spanResource[IO](span, "updateDateAccessed")
          .use(_ => apiCall)
      )
    } yield StatusCodes.Accepted: ToResponseMarshallable

  implicit val createAzureDiskReqDecoder: Decoder[CreateAzureDiskRequest] =
    Decoder.forProduct4("labels", "name", "size", "diskType")(CreateAzureDiskRequest.apply)

  implicit val createAzureRuntimeRequestDecoder: Decoder[CreateAzureRuntimeRequest] = Decoder.instance { c =>
    for {
      labels <- c.downField("labels").as[LabelMap]
      machineSize <- c.downField("machineSize").as[VirtualMachineSizeTypes]
      customEnvVars <- c
        .downField("customEnvironmentVariables")
        .as[Option[Map[String, String]]]
      azureDiskReq <- c.downField("disk").as[CreateAzureDiskRequest]
      apt <- c.downField("autopauseThreshold").as[Option[Int]]
    } yield CreateAzureRuntimeRequest(labels, machineSize, customEnvVars.getOrElse(Map.empty), azureDiskReq, apt)
  }

  implicit val updateAzureRuntimeRequestDecoder: Decoder[UpdateAzureRuntimeRequest] =
    Decoder.forProduct1("machineSize")(UpdateAzureRuntimeRequest.apply)

}
