package org.broadinstitute.dsde.workbench.leonardo.http.api

import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server
import akka.http.scaladsl.server.Directives.{complete, get, pathEndOrSingleSlash, pathPrefix}
import org.broadinstitute.dsde.workbench.leonardo.http.service.HelloService

class HelloRoutes(helloService: HelloService) {
  val routes: server.Route =
    pathPrefix("hello") {
      pathEndOrSingleSlash {
        get {
          complete(StatusCodes.OK -> helloService.getResponse)
        }
      }
    }
}
