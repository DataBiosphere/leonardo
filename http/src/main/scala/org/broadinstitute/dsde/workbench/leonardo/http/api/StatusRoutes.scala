package org.broadinstitute.dsde.workbench.leonardo.http
package api

import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server
import akka.http.scaladsl.server.Directives._
import de.heikoseeberger.akkahttpcirce.ErrorAccumulatingCirceSupport._
import io.circe.{Encoder, KeyEncoder}
import io.circe.syntax._
import org.broadinstitute.dsde.workbench.leonardo.http.service.StatusService
import org.broadinstitute.dsde.workbench.util.health.Subsystems.Subsystem
import org.broadinstitute.dsde.workbench.util.health.{StatusCheckResponse, SubsystemStatus}

import scala.concurrent.ExecutionContext

object BuildTimeVersion {
  val version = Option(getClass.getPackage.getImplementationVersion)
  val versionJson = Map("version" -> version.getOrElse("n/a")).asJson
}

class StatusRoutes(statusService: StatusService)(implicit executionContext: ExecutionContext) {
  implicit val subsystemEncoder: KeyEncoder[Subsystem] = KeyEncoder.encodeKeyString.contramap(_.value)
  implicit val subsystemStatusEncoder: Encoder[SubsystemStatus] =
    Encoder.forProduct2("ok", "messages")(x => SubsystemStatus.unapply(x).get)
  implicit val statusCheckResponseEncoder: Encoder[StatusCheckResponse] =
    Encoder.forProduct2("ok", "systems")(x => StatusCheckResponse.unapply(x).get)

  val route: server.Route =
    pathPrefix("status") {
      pathEndOrSingleSlash {
        get {
          complete(statusService.getStatus().map { statusResponse =>
            val httpStatus = if (statusResponse.ok) StatusCodes.OK else StatusCodes.InternalServerError
            httpStatus -> statusResponse
          })
        }
      }
    } ~
      pathPrefix("version") {
        pathEndOrSingleSlash {
          get {
            complete((StatusCodes.OK, BuildTimeVersion.versionJson))
          }
        }
      }
}
