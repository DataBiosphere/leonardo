package org.broadinstitute.dsde.workbench.leonardo.api

import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.LazyLogging
import net.ceedubs.ficus.Ficus._
import org.broadinstitute.dsde.workbench.leonardo.model.{ClusterName, GoogleProject}
import org.broadinstitute.dsde.workbench.leonardo.service.ProxyService
import scala.concurrent.ExecutionContext.Implicits.global

/**
  * Created by rtitle on 8/4/17.
  */
trait ProxyRoutes { self: LazyLogging =>
  val proxyService: ProxyService
  protected val tokenCookieName = "FCToken"
  private val config = ConfigFactory.parseResources("leonardo.conf").withFallback(ConfigFactory.load())
  private val whitelist = config.as[(List[String])]("whitelist")

  protected val proxyRoutes: Route =
    pathPrefix("notebooks" / Segment / Segment) { (googleProject, clusterName) =>
      extractRequest { request =>
        cookie(tokenCookieName) { tokenCookie => // rejected with MissingCookieRejection if the cookie is not present
          complete {
            proxyService.getCachedEmailFromToken(tokenCookie.value).flatMap { email =>
              if (whitelist.contains(email)) {
                // Proxy logic handled by the ProxyService class
                proxyService.proxy(GoogleProject(googleProject), ClusterName(clusterName), request, tokenCookie)
              } else throw AuthorizationError(email)
            }
          }
        }
      }
    }

}
