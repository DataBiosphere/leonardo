package org.broadinstitute.dsde.workbench.leonardo
package http
package api

import akka.http.scaladsl.server
import akka.http.scaladsl.model.StatusCodes
import cats.effect.IO
import akka.http.scaladsl.server.Directives._

class LivelinessRoutes {

  val route: server.Route =
    pathPrefix("liveliness") {
      pathEndOrSingleSlash {
        get {
          complete {
            IO(StatusCodes.OK)
          }
        }
      }
    }

}
