package org.broadinstitute.dsde.workbench.leonardo
package http
package api

import java.util.UUID

import akka.http.scaladsl.marshalling.ToResponseMarshallable
import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server
import akka.http.scaladsl.server.{Directive, Directive1}
import akka.http.scaladsl.server.Directives._
import cats.effect.{IO, Timer}
import de.heikoseeberger.akkahttpcirce.ErrorAccumulatingCirceSupport._
import cats.mtl.ApplicativeAsk
import io.circe.{Decoder, Encoder}
import org.broadinstitute.dsde.workbench.google2.{DiskName, ZoneName}
import org.broadinstitute.dsde.workbench.leonardo.JsonCodec._
import org.broadinstitute.dsde.workbench.leonardo.SamResource.PersistentDiskSamResource
import org.broadinstitute.dsde.workbench.leonardo.api.CookieSupport
import org.broadinstitute.dsde.workbench.model.google.GoogleProject
import org.broadinstitute.dsde.workbench.leonardo.http.api.DiskRoutes._
import org.broadinstitute.dsde.workbench.leonardo.model.RequestValidationError
import org.broadinstitute.dsde.workbench.model.{TraceId, UserInfo, WorkbenchEmail}

class DiskRoutes(diskService: DiskService[IO], userInfoDirectives: UserInfoDirectives)(
  implicit timer: Timer[IO]
) {
  val routes: server.Route = userInfoDirectives.requireUserInfo { userInfo =>
    CookieSupport.setTokenCookie(userInfo, CookieSupport.tokenCookieName) {
      implicit val traceId = ApplicativeAsk.const[IO, TraceId](TraceId(UUID.randomUUID()))
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
      context <- AppContext.generate[IO]()
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
      context <- AppContext.generate[IO]()
      implicit0(ctx: ApplicativeAsk[IO, AppContext]) = ApplicativeAsk.const[IO, AppContext](
        context
      )
      resp <- diskService.getDisk(userInfo, googleProject, diskName)
    } yield StatusCodes.OK -> resp

  private[api] def listDisksHandler(userInfo: UserInfo,
                                    googleProject: Option[GoogleProject],
                                    params: Map[String, String]): IO[ToResponseMarshallable] =
    for {
      context <- AppContext.generate[IO]()
      implicit0(ctx: ApplicativeAsk[IO, AppContext]) = ApplicativeAsk.const[IO, AppContext](
        context
      )
      resp <- diskService.listDisks(userInfo, googleProject, params)
    } yield StatusCodes.OK -> resp

  private[api] def deleteDiskHandler(userInfo: UserInfo,
                                     googleProject: GoogleProject,
                                     diskName: DiskName): IO[ToResponseMarshallable] =
    for {
      context <- AppContext.generate[IO]()
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
      context <- AppContext.generate[IO]()
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
      l <- x.downField("labels").as[Option[LabelMap]]
      us <- x.downField("size").as[DiskSize]
    } yield UpdateDiskRequest(l.getOrElse(Map.empty), us)
  }

  implicit val getDiskResponseEncoder: Encoder[GetPersistentDiskResponse] = Encoder.forProduct12(
    "id",
    "googleProject",
    "zone",
    "name",
    "googleId",
    "serviceAccount",
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
      x.serviceAccount,
      x.status,
      x.auditInfo,
      x.size,
      x.diskType,
      x.blockSize,
      x.labels
    )
  )

  implicit val listDiskResponseEncoder: Encoder[ListPersistentDiskResponse] = Encoder.forProduct9(
    "id",
    "googleProject",
    "zone",
    "name",
    "status",
    "auditInfo",
    "size",
    "diskType",
    "blockSize"
  )(x =>
    (
      x.id,
      x.googleProject,
      x.zone,
      x.name,
      x.status,
      x.auditInfo,
      x.size,
      x.diskType,
      x.blockSize
    )
  )

  private val diskNameReg = "([a-z|0-9|-])*".r

  private def validateDiskName(diskNameString: String): Either[Throwable, DiskName] =
    diskNameString match {
      case diskNameReg(_) => Right(DiskName(diskNameString))
      case _ =>
        Left(
          RequestValidationError(
            s"invalid disk name ${diskNameString}. Only lowercase alphanumeric characters, numbers and dashes are allowed in disk name"
          )
        )
    }

  def validateDiskNameDirective(diskNameString: String): Directive1[DiskName] =
    Directive { inner =>
      validateDiskName(diskNameString) match {
        case Left(e)  => failWith(e)
        case Right(c) => inner(Tuple1(c))
      }
    }

}

final case class ListPersistentDiskResponse(id: DiskId,
                                            googleProject: GoogleProject,
                                            zone: ZoneName,
                                            name: DiskName,
                                            status: DiskStatus,
                                            auditInfo: AuditInfo,
                                            size: DiskSize,
                                            diskType: DiskType,
                                            blockSize: BlockSize)

final case class GetPersistentDiskResponse(id: DiskId,
                                           googleProject: GoogleProject,
                                           zone: ZoneName,
                                           name: DiskName,
                                           googleId: Option[GoogleId],
                                           serviceAccount: WorkbenchEmail,
                                           samResource: PersistentDiskSamResource,
                                           status: DiskStatus,
                                           auditInfo: AuditInfo,
                                           size: DiskSize,
                                           diskType: DiskType,
                                           blockSize: BlockSize,
                                           labels: LabelMap)

final case class UpdateDiskRequest(labels: LabelMap, size: DiskSize)
