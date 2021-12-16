package org.broadinstitute.dsde.workbench.leonardo.http
package api

import akka.http.scaladsl.server
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.directives.MethodDirectives.get
import akka.util.ByteString
import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.dsde.workbench.leonardo.config.SwaggerConfig
import akka.stream.scaladsl.Flow

class SwaggerRoutes(swaggerConfig: SwaggerConfig) extends LazyLogging {
  private val swaggerUiPath = "META-INF/resources/webjars/swagger-ui/4.1.3"

  val routes: server.Route = {
    path("") {
      get {
        serveIndex
      }
    } ~
      path("api-docs.yaml") {
        get {
          getFromResource("swagger/api-docs.yaml")
        }
      } ~
      // We have to be explicit about the paths here since we're matching at the root URL and we don't
      // want to catch all paths lest we circumvent Spray's not-found and method-not-allowed error
      // messages.
      (pathPrefixTest("swagger-ui") | pathPrefixTest("oauth2") | pathSuffixTest("js")
        | pathSuffixTest("css") | pathPrefixTest("favicon")) {
        get {
          getFromResourceDirectory(swaggerUiPath)
        }
      }
  }

  private val serveIndex: server.Route = {
    val swaggerOptions =
      s"""
         |        validatorUrl: null,
         |        apisSorter: "alpha",
         |        operationsSorter: "alpha"
      """.stripMargin

    mapResponseEntity { entityFromJar =>
      entityFromJar.transformDataBytes(Flow.fromFunction[ByteString, ByteString] { original: ByteString =>
        ByteString(
          original.utf8String
            .replace("""url: "https://petstore.swagger.io/v2/swagger.json"""", "url: '/api-docs.yaml'")
            .replace("""layout: "StandaloneLayout"""", s"""layout: "StandaloneLayout", $swaggerOptions""")
            .replace(
              "window.ui = ui",
              s"""ui.initOAuth({
                 |        clientId: "${swaggerConfig.googleClientId}",
                 |        clientSecret: "${swaggerConfig.realm}",
                 |        realm: "${swaggerConfig.realm}",
                 |        appName: "Leonardo",
                 |        scopeSeparator: " ",
                 |        additionalQueryStringParams: {}
                 |      })
                 |      window.ui = ui
                 |      """.stripMargin
            )
        )
      })
    } {
      getFromResource(s"$swaggerUiPath/index.html")
    }
  }

}
