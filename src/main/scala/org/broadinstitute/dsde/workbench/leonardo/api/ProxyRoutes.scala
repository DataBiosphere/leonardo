package org.broadinstitute.dsde.workbench.leonardo.api

import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.dsde.workbench.leonardo.model.ClusterName
import org.broadinstitute.dsde.workbench.leonardo.service.ProxyService
import org.broadinstitute.dsde.workbench.model.WorkbenchEmail
import org.broadinstitute.dsde.workbench.model.google.GoogleProject

import scala.concurrent.ExecutionContext

/**
  * Created by rtitle on 8/4/17.
  */
trait ProxyRoutes extends UserInfoDirectives{ self: LazyLogging =>
  val proxyService: ProxyService
  implicit val executionContext: ExecutionContext

  protected val tokenCookieName = "FCtoken"

  protected val proxyRoutes: Route =
    pathPrefix("notebooks" / Segment / Segment) { (googleProject, clusterName) =>
      extractRequest { request =>
        cookie(tokenCookieName) { tokenCookie => // rejected with MissingCookieRejection if the cookie is not present
          complete {
            proxyService.getCachedUserInfoFromToken(tokenCookie.value).flatMap { userInfo =>
              // Proxy logic handled by the ProxyService class
              proxyService.proxy(userInfo, GoogleProject(googleProject), ClusterName(clusterName), request, tokenCookie)
            }
          }
        }
      }
    }

}
