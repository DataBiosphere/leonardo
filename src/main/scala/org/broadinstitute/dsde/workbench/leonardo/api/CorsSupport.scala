package org.broadinstitute.dsde.workbench.leonardo.api

import akka.http.scaladsl.model.{HttpResponse, StatusCodes}
import akka.http.scaladsl.model.headers._
import akka.http.scaladsl.model.HttpMethods._
import akka.http.scaladsl.server.{Directive0, Route}
import akka.http.scaladsl.server.Directives._
import org.broadinstitute.dsde.workbench.leonardo.service.AuthorizationError

/**
  * Created by rtitle on 1/3/18.
  */
trait CorsSupport {
  def corsHandler(r: Route) =
    addAccessControlHeaders {
      preflightRequestHandler ~ r
    }

  // This handles preflight OPTIONS requests.
  private def preflightRequestHandler: Route = options {
    complete(HttpResponse(StatusCodes.NoContent).withHeaders(`Access-Control-Allow-Methods`(OPTIONS, POST, PUT, GET, DELETE, HEAD, PATCH)))
  }

  // This directive adds access control headers to normal responses
  private def addAccessControlHeaders: Directive0 = {
    optionalHeaderValueByType[`Origin`](()) flatMap {
      case Some(origin) => respondWithHeaders(
        `Access-Control-Allow-Origin`(origin.value),
        `Access-Control-Allow-Credentials`(true),
        `Access-Control-Allow-Headers`("Authorization", "Content-Type", "Accept", "Origin"),
        `Access-Control-Max-Age`(1728000))
      case None =>
        failWith(AuthorizationError())
    }

  }

}
