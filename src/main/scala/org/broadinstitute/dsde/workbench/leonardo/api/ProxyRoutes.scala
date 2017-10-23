package org.broadinstitute.dsde.workbench.leonardo.api

import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.LazyLogging
import net.ceedubs.ficus.Ficus._
import org.broadinstitute.dsde.workbench.leonardo.model.{ClusterName, GoogleProject}
import org.broadinstitute.dsde.workbench.leonardo.service.ProxyService
import org.broadinstitute.dsde.workbench.model.WorkbenchUserEmail

import scala.concurrent.ExecutionContext.Implicits.global

/**
  * Created by rtitle on 8/4/17.
  */
trait ProxyRoutes extends UserInfoDirectives{ self: LazyLogging =>
  val proxyService: ProxyService
  val whiteListConfig: Set[WorkbenchUserEmail]

  protected val tokenCookieName = "FCToken"

  protected val proxyRoutes: Route =
    pathPrefix("notebooks" / Segment / Segment) { (googleProject, clusterName) =>
      extractRequest { request =>
        cookie(tokenCookieName) { tokenCookie => // rejected with MissingCookieRejection if the cookie is not present
          complete {
            proxyService.getCachedEmailFromToken(tokenCookie.value).flatMap { email =>
              if (whiteListConfig.contains(email)) {
                // Proxy logic handled by the ProxyService class
                proxyService.proxy(GoogleProject(googleProject), ClusterName(clusterName), request, tokenCookie)
              } else throw AuthorizationError(email)
            }
          }
        }
      }
    }

}
