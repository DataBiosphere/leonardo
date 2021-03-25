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
  // Note: change to "localhost" if running tests locally
  val host = java.net.InetAddress.getLocalHost.getHostName
  val port = 9099

  // Use a separate dispatcher than other RestClients:
  // https://github.com/broadinstitute/workbench-libs/blob/develop/serviceTest/src/test/resources/application.conf
  implicit val dispatcher = system.dispatchers.lookup("blocking-dispatcher")

  def get(rurl: String): String =
    s"http://$host:$port/proxyRedirectClient?rurl=${rurl}"

  def startServer: Future[Http.ServerBinding] = {
    logger.info(s"Starting local server on 0.0.0.0:$port")
    Http().newServerAt("0.0.0.0", port).bind(route)
  }

  def stopServer(serverBinding: Http.ServerBinding): Future[Done] = {
    logger.info("Stopping local server")
    serverBinding.unbind()
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

  private def getContent(rurl: String): String = {
    val resourceFile = ResourceFile("redirect-proxy-page.html")
    Source.fromFile(resourceFile).mkString.replace("rurl", rurl)
  }
}
