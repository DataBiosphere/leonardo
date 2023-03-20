package org.broadinstitute.dsde.workbench.leonardo
package http
package api

import akka.http.scaladsl.marshalling.ToResponseMarshallable
import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server
import akka.http.scaladsl.server.Directives._
import cats.effect.IO
import cats.mtl.Ask
import io.circe.Encoder
import org.broadinstitute.dsde.workbench.leonardo.JsonCodec._
import io.opencensus.scala.akka.http.TracingDirective.traceRequestForService
import org.broadinstitute.dsde.workbench.leonardo.http.api.DiskV2Routes._
import org.broadinstitute.dsde.workbench.leonardo.http.service.DiskV2Service
import org.broadinstitute.dsde.workbench.model.{TraceId, UserInfo}
import org.broadinstitute.dsde.workbench.openTelemetry.OpenTelemetryMetrics

import java.util.UUID

class DiskV2Routes(diskV2Service: DiskV2Service[IO], userInfoDirectives: UserInfoDirectives)(implicit
  metrics: OpenTelemetryMetrics[IO]
) {
  val routes: server.Route = traceRequestForService(serviceData) { span =>
    extractAppContext(Some(span)) { implicit ctx =>
      userInfoDirectives.requireUserInfo { userInfo =>
        CookieSupport.setTokenCookie(userInfo) {
          implicit val traceId = Ask.const[IO, TraceId](TraceId(UUID.randomUUID()))
          pathPrefix("v2" / "disks") {
            pathPrefix(workspaceIdSegment) { workspaceId =>
              pathEndOrSingleSlash {
                pathPrefix("azure") {
                  pathPrefix(diskNameSegmentWithValidation) { diskName =>
                    pathEndOrSingleSlash {
                      get {
                        complete(
                          getAzureDiskHandler(userInfo, workspaceId, diskName)
                        )
                      } ~ delete {
                        complete(
                          deleteAzureDiskHandler(userInfo, workspaceId, diskName)
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

  private[api] def getAzureDiskHandler(userInfo: UserInfo, workspaceId: WorkspaceId, diskName: DiskName)(implicit
    ev: Ask[IO, AppContext]
  ): IO[ToResponseMarshallable] =
    for {
      ctx <- ev.ask[AppContext]
      apiCall = diskV2Service.getDisk(userInfo, workspaceId, diskName)
      _ <- metrics.incrementCounter("getDiskV2")
      resp <- ctx.span.fold(apiCall)(span => spanResource[IO](span, "getDiskV2").use(_ => apiCall))
    } yield StatusCodes.OK -> resp: ToResponseMarshallable

  private[api] def deleteAzureDiskHandler(userInfo: UserInfo, workspaceId: WorkspaceId, diskName: DiskName)(implicit
    ev: Ask[IO, AppContext]
  ): IO[ToResponseMarshallable] =
    for {
      ctx <- ev.ask[AppContext]
      apiCall = diskV2Service.deleteDisk(userInfo, workspaceId, diskName)
      _ <- metrics.incrementCounter("deleteDiskV2")
      _ <- ctx.span.fold(apiCall)(span => spanResource[IO](span, "deleteDiskV2").use(_ => apiCall))
    } yield StatusCodes.Accepted: ToResponseMarshallable
}

object DiskV2Routes {
  implicit val getPersistentDiskResponseEncoder: Encoder[GetPersistentDiskResponse] = Encoder.forProduct12(
    "id",
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
    "formattedBy"
  )(x =>
    (
      x.id,
      x.cloudContext,
      x.zone,
      x.name.asString,
      x.serviceAccount,
      x.status,
      x.auditInfo,
      x.size,
      x.diskType,
      x.blockSize,
      x.labels,
      x.formattedBy
    )
  )
}
