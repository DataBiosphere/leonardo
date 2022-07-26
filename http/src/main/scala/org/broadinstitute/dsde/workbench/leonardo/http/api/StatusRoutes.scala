package org.broadinstitute.dsde.workbench.leonardo.http
package api

import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server
import akka.http.scaladsl.server.Directives._
import de.heikoseeberger.akkahttpcirce.ErrorAccumulatingCirceSupport._
import io.circe.syntax._
import org.broadinstitute.dsde.workbench.leonardo.http.service.StatusService

import scala.concurrent.ExecutionContext

object BuildTimeVersion {
  val version = Option(getClass.getPackage.getImplementationVersion)
  val versionJson = Map("version" -> version.getOrElse("n/a")).asJson
}

class StatusRoutes(statusService: StatusService)(implicit executionContext: ExecutionContext) {

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
