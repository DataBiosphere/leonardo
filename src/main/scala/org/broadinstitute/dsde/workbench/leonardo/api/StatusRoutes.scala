package org.broadinstitute.dsde.workbench.leonardo.api

import akka.http.scaladsl.server
import akka.http.scaladsl.server.Directives._
import org.broadinstitute.dsde.workbench.leonardo.service.StatusService
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
import akka.http.scaladsl.model.StatusCodes
import org.broadinstitute.dsde.workbench.util.health.StatusJsonSupport._
import scala.concurrent.ExecutionContext

trait StatusRoutes {
  val statusService: StatusService
  implicit val executionContext: ExecutionContext

  def statusRoutes: server.Route =
    pathPrefix("status") {
      pathEndOrSingleSlash {
        get {
          complete(statusService.getStatus().map { statusResponse =>
            val httpStatus = if (statusResponse.ok) StatusCodes.OK else StatusCodes.InternalServerError
            httpStatus -> statusResponse
          })
        }
      }
    }
}
