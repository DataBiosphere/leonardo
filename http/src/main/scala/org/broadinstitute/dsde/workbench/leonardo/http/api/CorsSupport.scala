package org.broadinstitute.dsde.workbench.leonardo
package http
package api

import akka.http.scaladsl.model.HttpRequest
import akka.http.scaladsl.model.headers._
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.directives.DebuggingDirectives
import akka.http.scaladsl.server.{Directive0, Route}
import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.dsde.workbench.leonardo.config.{ContentSecurityPolicyConfig, RefererConfig}

class CorsSupport(contentSecurityPolicy: ContentSecurityPolicyConfig, refererConfig: RefererConfig)
    extends LazyLogging {

  def corsHandler(r: Route): Route =
    addAccessControlHeaders {
      validateOrigin {
        r
      }
    }

  // Ensure the request Origin is included in our referrer allowlist.
  private def validateOrigin: Directive0 =
    if (!refererConfig.enabled || refererConfig.validHosts.contains("*")) {
      pass
    } else {
      def validOrigins: Set[HttpOrigin] = refererConfig.validHosts
        .flatMap { uriString =>
          Set(HttpOrigin(s"http://${uriString}"), HttpOrigin(s"https://${uriString}"))
        }

      checkSameOrigin(HttpOriginRange(validOrigins.toSeq: _*))
    }

  private def requestPath(req: HttpRequest): String = req.uri.toString
  private val logRequestPath = DebuggingDirectives.logRequest(requestPath _)

  // This directive adds access control headers to normal responses
  private def addAccessControlHeaders: Directive0 =
    headerValueByType(Origin).flatMap { origin =>
      mapResponseHeaders { headers =>
        // Filter out the Access-Control-Allow-Origin set by Jupyter so we don't have duplicate headers
        // (causes issues on some browsers). See https://github.com/DataBiosphere/leonardo/issues/272
        headers.filter { h =>
          h.isNot(`Access-Control-Allow-Origin`.lowercaseName) && h.isNot("content-security-policy")
        } ++
          Seq(
            `Access-Control-Allow-Origin`(origin.value),
            RawHeader("Vary", Origin.name),
            `Access-Control-Allow-Credentials`(true),
            `Access-Control-Allow-Headers`("Authorization", "Content-Type", "Accept", "Origin", "X-App-Id"),
            `Access-Control-Max-Age`(1728000),
            // There are no akka-http model objects for Content-Security-Policy. See:
            // https://github.com/akka/akka-http/issues/155
            RawHeader("Content-Security-Policy", contentSecurityPolicy.asString)
          )
      }
    }
}
