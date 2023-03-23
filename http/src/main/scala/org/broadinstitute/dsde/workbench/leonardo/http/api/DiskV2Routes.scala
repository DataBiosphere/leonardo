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
import io.circe.Encoder
import org.broadinstitute.dsde.workbench.leonardo.JsonCodec._
import io.opencensus.scala.akka.http.TracingDirective.traceRequestForService
import org.broadinstitute.dsde.workbench.leonardo.http.api.DiskV2Routes._
import org.broadinstitute.dsde.workbench.leonardo.http.service.DiskV2Service
import org.broadinstitute.dsde.workbench.model.{TraceId, UserInfo}
import org.broadinstitute.dsde.workbench.openTelemetry.OpenTelemetryMetrics

class DiskV2Routes(diskV2Service: DiskV2Service[IO], userInfoDirectives: UserInfoDirectives)(implicit
  metrics: OpenTelemetryMetrics[IO]
) {
  val routes: server.Route = traceRequestForService(serviceData) { span =>
    extractAppContext(Some(span)) { implicit ctx =>
      userInfoDirectives.requireUserInfo { userInfo =>
        CookieSupport.setTokenCookie(userInfo) {
          implicit val traceId = Ask.const[IO, TraceId](TraceId(UUID.randomUUID()))
          pathPrefix("v2" / "disks" / workspaceIdSegment) { workspaceId =>
            pathPrefix(diskIdSegment) { diskId =>
              pathEndOrSingleSlash {
                get {
                  complete(
                    getDiskV2Handler(userInfo, workspaceId, diskId)
                  )
                } ~ delete {
                  complete(
                    deleteDiskV2Handler(userInfo, workspaceId, diskId)
                  )
                }
              }
            }
          }
        }
      }
    }
  }

  private[api] def getDiskV2Handler(userInfo: UserInfo, workspaceId: WorkspaceId, diskId: DiskId)(implicit
    ev: Ask[IO, AppContext]
  ): IO[ToResponseMarshallable] =
    for {
      ctx <- ev.ask[AppContext]
      apiCall = diskV2Service.getDisk(userInfo, workspaceId, diskId)
      _ <- metrics.incrementCounter("getDiskV2")
      resp <- ctx.span.fold(apiCall)(span =>
        spanResource[IO](span, "getDiskV2")
          .use(_ => apiCall)
      )
    } yield StatusCodes.OK -> resp: ToResponseMarshallable

  private[api] def deleteDiskV2Handler(userInfo: UserInfo, workspaceId: WorkspaceId, diskId: DiskId)(implicit
    ev: Ask[IO, AppContext]
  ): IO[ToResponseMarshallable] =
    for {
      ctx <- ev.ask[AppContext]
      apiCall = diskV2Service.deleteDisk(userInfo, workspaceId, diskId)
      _ <- metrics.incrementCounter("deleteDiskV2")
      _ <- ctx.span.fold(apiCall)(span => spanResource[IO](span, "deleteDiskV2").use(_ => apiCall))
    } yield StatusCodes.Accepted
}

object DiskV2Routes {
  implicit val getPersistentDiskResponseEncoder: Encoder[GetPersistentDiskResponse] = Encoder.forProduct13(
    "id",
    "cloudContext",
    "zone",
    "name",
    "serviceAccount",
    "samResource",
    "status",
    "auditInfo",
    "size",
    "diskType",
    "blockSize",
    "labels",
    "formattedBy"
  )(x => GetPersistentDiskResponse.unapply(x).get)
}
