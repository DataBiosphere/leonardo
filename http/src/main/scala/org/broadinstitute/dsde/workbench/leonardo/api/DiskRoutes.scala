package org.broadinstitute.dsde.workbench.leonardo
package http
package api

import java.util.UUID

import akka.http.scaladsl.marshalling.ToResponseMarshallable
import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server
import akka.http.scaladsl.server.Directives._
import cats.effect.{IO, Timer}
import de.heikoseeberger.akkahttpcirce.ErrorAccumulatingCirceSupport._
import cats.mtl.ApplicativeAsk
import io.circe.{Decoder, Encoder}
import org.broadinstitute.dsde.workbench.google2.DiskName
import org.broadinstitute.dsde.workbench.leonardo.JsonCodec._
import org.broadinstitute.dsde.workbench.leonardo.api.CookieSupport
import org.broadinstitute.dsde.workbench.leonardo.http.api.LeoRoutes.validateDiskNameDirective
import org.broadinstitute.dsde.workbench.model.google.GoogleProject
import org.broadinstitute.dsde.workbench.leonardo.http.api.DiskRoutes._
import org.broadinstitute.dsde.workbench.model.{TraceId, UserInfo}

class DiskRoutes(diskService: DiskService[IO], userInfoDirectives: UserInfoDirectives)(
  implicit timer: Timer[IO]
) {
  val routes: server.Route = userInfoDirectives.requireUserInfo { userInfo =>
    CookieSupport.setTokenCookie(userInfo, CookieSupport.tokenCookieName) {
      implicit val traceId = ApplicativeAsk.const[IO, TraceId](TraceId(UUID.randomUUID()))
      pathPrefix("google" / "v1" / "disk") {
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
            pathEndOrSingleSlash {
              parameterMap { params =>
                get {
                  complete(
                    listDisksHandler(
                      userInfo,
                      Some(googleProject),
                      params
                    )
                  )
                }
              }
            } ~
              pathPrefix(Segment) { diskNameString =>
                validateDiskNameDirective(diskNameString) { diskName =>
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
                            googleProject,
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

  private[api] def createDiskHandler(userInfo: UserInfo,
                                     googleProject: GoogleProject,
                                     diskName: DiskName,
                                     req: CreateDiskRequest): IO[ToResponseMarshallable] =
    for {
      context <- AppContext.generate[IO]
      implicit0(ctx: ApplicativeAsk[IO, AppContext]) = ApplicativeAsk.const[IO, AppContext](
        context
      )
      _ <- diskService.createDisk(
        userInfo,
        googleProject,
        diskName,
        req
      )
    } yield StatusCodes.Accepted

  private[api] def getDiskHandler(userInfo: UserInfo,
                                  googleProject: GoogleProject,
                                  diskName: DiskName): IO[ToResponseMarshallable] =
    for {
      context <- AppContext.generate[IO]
      implicit0(ctx: ApplicativeAsk[IO, AppContext]) = ApplicativeAsk.const[IO, AppContext](
        context
      )
      resp <- diskService.getDisk(userInfo, googleProject, diskName)
    } yield StatusCodes.OK -> resp

  private[api] def listDisksHandler(userInfo: UserInfo,
                                    googleProject: Option[GoogleProject],
                                    params: Map[String, String]): IO[ToResponseMarshallable] =
    for {
      context <- AppContext.generate[IO]
      implicit0(ctx: ApplicativeAsk[IO, AppContext]) = ApplicativeAsk.const[IO, AppContext](
        context
      )
      resp <- diskService.listDisks(userInfo, googleProject, params)
    } yield StatusCodes.OK -> resp

  private[api] def deleteDiskHandler(userInfo: UserInfo,
                                     googleProject: GoogleProject,
                                     diskName: DiskName): IO[ToResponseMarshallable] =
    for {
      context <- AppContext.generate[IO]
      implicit0(ctx: ApplicativeAsk[IO, AppContext]) = ApplicativeAsk.const[IO, AppContext](
        context
      )
      _ <- diskService.deleteDisk(userInfo, googleProject, diskName)
    } yield StatusCodes.Accepted

  private[api] def updateDiskHandler(userInfo: UserInfo,
                                     googleProject: GoogleProject,
                                     diskName: DiskName,
                                     req: UpdateDiskRequest): IO[ToResponseMarshallable] =
    for {
      context <- AppContext.generate[IO]
      implicit0(ctx: ApplicativeAsk[IO, AppContext]) = ApplicativeAsk.const[IO, AppContext](
        context
      )
      _ <- diskService.updateDisk(userInfo, googleProject, diskName, req)
    } yield StatusCodes.Accepted
}

object DiskRoutes {

  implicit val createDiskRequestDecoder: Decoder[CreateDiskRequest] = Decoder.instance { c =>
    for {
      l <- c.downField("labels").as[Option[LabelMap]]
      s <- c.downField("size").as[Option[DiskSize]]
      t <- c.downField("diskType").as[Option[DiskType]]
      bs <- c.downField("blockSize").as[Option[BlockSize]]
    } yield CreateDiskRequest(
      l.getOrElse(Map.empty),
      s,
      t,
      bs
    )
  }

  implicit val updateDiskRequestDecoder: Decoder[UpdateDiskRequest] = Decoder.instance { x =>
    for {
      l <- x.downField("labels").as[LabelMap]
      us <- x.downField("updateSize").as[Option[DiskSize]]
      ud <- x.downField("updateDiskType").as[Option[DiskType]]
      ub <- x.downField("updateBlockSize").as[Option[BlockSize]]
    } yield UpdateDiskRequest(l, us, ud, ub)
  }

  implicit val diskEncoder: Encoder[PersistentDisk] = Encoder.forProduct12(
    "id",
    "googleProject",
    "zone",
    "name",
    "googleId",
    "samResourceId",
    "status",
    "auditInfo",
    "size",
    "diskType",
    "blockSize",
    "labels"
  )(x =>
    (
      x.id,
      x.googleProject,
      x.zone,
      x.name,
      x.googleId,
      x.samResourceId,
      x.status,
      x.auditInfo,
      x.size,
      x.diskType,
      x.blockSize,
      x.labels
    )
  )
}

final case class CreateDiskRequest(labels: LabelMap,
                                   size: Option[DiskSize],
                                   diskType: Option[DiskType],
                                   blockSize: Option[BlockSize])

final case class UpdateDiskRequest(labels: LabelMap,
                                   updateSize: Option[DiskSize],
                                   updateDiskType: Option[DiskType],
                                   updateBlockSize: Option[BlockSize])
