package org.broadinstitute.dsde.workbench.leonardo
package http
package api

import akka.http.scaladsl.model.HttpMethods._
import akka.http.scaladsl.model.headers._
import akka.http.scaladsl.model.{HttpResponse, StatusCodes}
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.{Directive0, Route}
import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.dsde.workbench.leonardo.config.{ContentSecurityPolicyConfig, RefererConfig}

class CorsSupport(contentSecurityPolicy: ContentSecurityPolicyConfig, refererConfig: RefererConfig)
    extends LazyLogging {

  def corsHandler(r: Route): Route =
    addAccessControlHeaders {
      concat (
        preflightRequestHandler,
        validateOrigin {
          r
        }
      )
    }

  // This handles preflight OPTIONS requests.
  private def preflightRequestHandler: Route =
    options {
      complete(
        HttpResponse(StatusCodes.NoContent)
          .withHeaders(`Access-Control-Allow-Methods`(OPTIONS, POST, PUT, GET, DELETE, HEAD, PATCH))
      )
    }

  // Ensure the request Origin is included in our referrer allowlist.
  private def validateOrigin: Directive0 =
    if (!refererConfig.enabled || refererConfig.validHosts.contains("*")) {
      pass
    } else {
      def validOrigins: Set[HttpOrigin] = refererConfig.validHosts
        .map(uriString => HttpOrigin(s"http://${uriString}"))
      checkSameOrigin(HttpOriginRange(validOrigins.toSeq: _*))
    }

  // This directive adds access control headers to normal responses
  private def addAccessControlHeaders: Directive0 =
    optionalHeaderValueByType(Origin) map {
      case Some(origin) =>
        Seq(`Access-Control-Allow-Origin`(origin.value), RawHeader("Vary", Origin.name))
      case None =>
        Seq(`Access-Control-Allow-Origin`.*)
    } flatMap { allowOrigin =>
      mapResponseHeaders { headers =>
        // Filter out the Access-Control-Allow-Origin set by Jupyter so we don't have duplicate headers
        // (causes issues on some browsers). See https://github.com/DataBiosphere/leonardo/issues/272
        headers.filter { h =>
          h.isNot(`Access-Control-Allow-Origin`.lowercaseName) && h.isNot("content-security-policy")
        } ++
          allowOrigin ++
          Seq(
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
