package org.broadinstitute.dsde.workbench.leonardo
package http
package api

import java.util.UUID
import akka.http.scaladsl.marshalling.ToResponseMarshallable
import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server
import akka.http.scaladsl.server.Directives._
import cats.effect.IO
import cats.mtl.Ask
import de.heikoseeberger.akkahttpcirce.ErrorAccumulatingCirceSupport._
import io.circe.{Decoder, Encoder}
import org.broadinstitute.dsde.workbench.google2.{DiskName, ZoneName}
import org.broadinstitute.dsde.workbench.leonardo.JsonCodec._
import io.opencensus.scala.akka.http.TracingDirective.traceRequestForService
import org.broadinstitute.dsde.workbench.leonardo.http.api.DiskRoutes._
import org.broadinstitute.dsde.workbench.leonardo.http.service.DiskService
import org.broadinstitute.dsde.workbench.model.google.GoogleProject
import org.broadinstitute.dsde.workbench.model.{TraceId, UserInfo}
import org.broadinstitute.dsde.workbench.openTelemetry.OpenTelemetryMetrics

class DiskRoutes(diskService: DiskService[IO], userInfoDirectives: UserInfoDirectives)(implicit
  metrics: OpenTelemetryMetrics[IO]
) {
  val routes: server.Route = traceRequestForService(serviceData) { span =>
    extractAppContext(Some(span)) { implicit ctx =>
      userInfoDirectives.requireUserInfo { userInfo =>
        CookieSupport.setTokenCookie(userInfo) {
          implicit val traceId = Ask.const[IO, TraceId](TraceId(UUID.randomUUID()))
          pathPrefix("google" / "v1" / "disks") {
            pathEndOrSingleSlash {
              parameterMap { params =>
                get {
                  complete(
                    listDisksHandler(
                      userInfo,
                      None,
                      params
                    )
                  )
                }
              }
            } ~
              pathPrefix(googleProjectSegment) { googleProject =>
                // TODO: use cloudContextSegment directly once cloudContext is used in all handlers
                val cloudContext = CloudContext.Gcp(googleProject)

                pathEndOrSingleSlash {
                  parameterMap { params =>
                    get {
                      complete(
                        listDisksHandler(
                          userInfo,
                          Some(cloudContext),
                          params
                        )
                      )
                    }
                  }
                } ~
                  pathPrefix(Segment) { diskNameString =>
                    RouteValidation.validateNameDirective(diskNameString, DiskName.apply) { diskName =>
                      pathEndOrSingleSlash {
                        post {
                          entity(as[CreateDiskRequest]) { req =>
                            complete(
                              createDiskHandler(
                                userInfo,
                                googleProject,
                                diskName,
                                req
                              )
                            )
                          }
                        } ~
                          get {
                            complete(
                              getDiskHandler(
                                userInfo,
                                cloudContext,
                                diskName
                              )
                            )
                          } ~
                          patch {
                            entity(as[UpdateDiskRequest]) { req =>
                              complete(
                                updateDiskHandler(
                                  userInfo,
                                  googleProject,
                                  diskName,
                                  req
                                )
                              )
                            }
                          } ~
                          delete {
                            complete(
                              deleteDiskHandler(
                                userInfo,
                                googleProject,
                                diskName
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

  private[api] def createDiskHandler(
    userInfo: UserInfo,
    googleProject: GoogleProject,
    diskName: DiskName,
    req: CreateDiskRequest
  )(implicit ev: Ask[IO, AppContext]): IO[ToResponseMarshallable] =
    for {
      ctx <- ev.ask[AppContext]
      apiCall = diskService.createDisk(userInfo, googleProject, diskName, req)
      _ <- metrics.incrementCounter("createDisk")
      _ <- ctx.span.fold(apiCall)(span => spanResource[IO](span, "createDisk").use(_ => apiCall))
    } yield StatusCodes.Accepted

  private[api] def getDiskHandler(userInfo: UserInfo, cloudContext: CloudContext, diskName: DiskName)(implicit
    ev: Ask[IO, AppContext]
  ): IO[ToResponseMarshallable] =
    for {
      ctx <- ev.ask[AppContext]
      apiCall = diskService.getDisk(userInfo, cloudContext, diskName)
      _ <- metrics.incrementCounter("getDisk")
      resp <- ctx.span.fold(apiCall)(span =>
        spanResource[IO](span, "getDisk")
          .use(_ => apiCall)
      )
    } yield StatusCodes.OK -> resp

  private[api] def listDisksHandler(
    userInfo: UserInfo,
    cloudContext: Option[CloudContext],
    params: Map[String, String]
  )(implicit ev: Ask[IO, AppContext]): IO[ToResponseMarshallable] =
    for {
      ctx <- ev.ask[AppContext]
      apiCall = diskService.listDisks(userInfo, cloudContext, params)
      _ <- metrics.incrementCounter("listDisks")
      resp <- ctx.span.fold(apiCall)(span => spanResource[IO](span, "listDisks").use(_ => apiCall))
    } yield StatusCodes.OK -> resp

  private[api] def deleteDiskHandler(userInfo: UserInfo, googleProject: GoogleProject, diskName: DiskName)(implicit
    ev: Ask[IO, AppContext]
  ): IO[ToResponseMarshallable] =
    for {
      ctx <- ev.ask[AppContext]
      apiCall = diskService.deleteDisk(userInfo, googleProject, diskName)
      _ <- metrics.incrementCounter("deleteDisk")
      _ <- ctx.span.fold(apiCall)(span => spanResource[IO](span, "deleteDisk").use(_ => apiCall))
    } yield StatusCodes.Accepted

  private[api] def updateDiskHandler(
    userInfo: UserInfo,
    googleProject: GoogleProject,
    diskName: DiskName,
    req: UpdateDiskRequest
  )(implicit ev: Ask[IO, AppContext]): IO[ToResponseMarshallable] =
    for {
      ctx <- ev.ask[AppContext]
      apiCall = diskService.updateDisk(userInfo, googleProject, diskName, req)
      _ <- metrics.incrementCounter("updateDisk")
      _ <- ctx.span.fold(apiCall)(span => spanResource[IO](span, "updateDisk").use(_ => apiCall))
    } yield StatusCodes.Accepted
}

object DiskRoutes {
  implicit val sourceDiskDecoder: Decoder[SourceDiskRequest] = Decoder.instance { x =>
    for {
      gp <- x.downField("googleProject").as[GoogleProject]
      n <- x.downField("name").as[DiskName]
    } yield SourceDiskRequest(gp, n)
  }

  implicit val createDiskRequestDecoder: Decoder[CreateDiskRequest] = Decoder.instance { c =>
    for {
      l <- c.downField("labels").as[Option[LabelMap]]
      s <- c.downField("size").as[Option[DiskSize]]
      t <- c.downField("diskType").as[Option[DiskType]]
      bs <- c.downField("blockSize").as[Option[BlockSize]]
      zone <- c.downField("zone").as[Option[ZoneName]]
      sourceDisk <- c.downField("sourceDisk").as[Option[SourceDiskRequest]]
    } yield CreateDiskRequest(
      l.getOrElse(Map.empty),
      s,
      t,
      bs,
      zone,
      sourceDisk
    )
  }

  implicit val updateDiskRequestDecoder: Decoder[UpdateDiskRequest] = Decoder.instance { x =>
    for {
      l <- x.downField("labels").as[Option[LabelMap]]
      us <- x.downField("size").as[DiskSize]
    } yield UpdateDiskRequest(l.getOrElse(Map.empty), us)
  }

  implicit val getPersistentDiskResponseEncoder: Encoder[GetPersistentDiskResponse] = Encoder.forProduct14(
    "id",
    "googleProject",
    "cloudContext",
    "zone",
    "name",
    "serviceAccount",
    "status",
    "auditInfo",
    "size",
    "diskType",
    "blockSize",
    "labels",
    "formattedBy",
    "workspaceId"
  )(x =>
    (
      x.id,
      x.cloudContext.asString,
      x.cloudContext,
      x.zone,
      x.name,
      x.serviceAccount,
      x.status,
      x.auditInfo,
      x.size,
      x.diskType,
      x.blockSize,
      x.labels,
      x.formattedBy,
      x.workspaceId
    )
  )

  implicit val listDiskResponseEncoder: Encoder[ListPersistentDiskResponse] = Encoder.forProduct12(
    "id",
    "googleProject",
    "cloudContext",
    "zone",
    "name",
    "status",
    "auditInfo",
    "size",
    "diskType",
    "blockSize",
    "labels",
    "workspaceId"
  )(x =>
    (
      x.id,
      x.cloudContext.asString,
      x.cloudContext,
      x.zone,
      x.name,
      x.status,
      x.auditInfo,
      x.size,
      x.diskType,
      x.blockSize,
      x.labels,
      x.workspaceId
    )
  )

}
