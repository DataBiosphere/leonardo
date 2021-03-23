package org.broadinstitute.dsde.workbench.leonardo

import akka.Done
import akka.http.scaladsl.Http
import akka.http.scaladsl.model._
import akka.http.scaladsl.server.Directives.{get => httpGet, _}
import akka.http.scaladsl.server.Route
import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.dsde.workbench.ResourceFile
import org.broadinstitute.dsde.workbench.service.RestClient

import scala.concurrent.Future
import scala.io.Source

object ProxyRedirectClient extends RestClient with LazyLogging {

  def get(rurl: String): String =
    s"http://localhost:9090/proxyRedirectClient?rurl=${rurl}"

  def startServer: Future[Http.ServerBinding] = {
    logger.info("Starting local server on port 9090")
    Http().newServerAt("localhost", 9090).bind(route)
  }

  def stopServer(bindingFuture: Future[Http.ServerBinding]): Future[Done] = {
    logger.info("Stopping local server")
    bindingFuture.flatMap(_.unbind())
  }

  val route: Route =
    path("proxyRedirectClient") {
      parameter("rurl") { rurl =>
        httpGet {
          complete {
            logger.info(s"Serving proxy redirect client for redirect url $rurl")
            HttpEntity(ContentTypes.`text/html(UTF-8)`, getContent(rurl))
          }
        }
      }
    }

  private def getContent(rurl: String) = {
    val resourceFile = ResourceFile("redirect-proxy-page.html")
    val raw = Source.fromFile(resourceFile).mkString

    val replacementMap = Map(
      "rurl" -> rurl
    )
    replacementMap.foldLeft(raw) {
      case (source, (key, replacement)) =>
        source.replace("$(" + key + ")", s"""'$replacement'""")
    }
  }
}
