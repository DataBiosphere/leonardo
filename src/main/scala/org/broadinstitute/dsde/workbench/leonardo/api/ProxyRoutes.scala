package org.broadinstitute.dsde.workbench.leonardo.api

import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.dsde.workbench.leonardo.service.ProxyService

/**
  * Created by rtitle on 8/4/17.
  */
trait ProxyRoutes { self: LazyLogging =>
  val proxyService: ProxyService

  protected val proxyRoutes: Route =
    pathPrefix("notebooks" / Segment / Segment) { (googleProject, clusterName) =>
      extractRequest { request =>
        complete {
          // Proxy logic handled by the ProxyService class
          proxyService.proxy(googleProject, clusterName, request)
        }
      }
    }
}
