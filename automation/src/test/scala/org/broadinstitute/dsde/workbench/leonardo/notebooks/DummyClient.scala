package org.broadinstitute.dsde.workbench.leonardo.notebooks

import akka.Done
import akka.http.scaladsl.server.Route
import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.dsde.workbench.ResourceFile
import org.broadinstitute.dsde.workbench.auth.AuthToken
import org.broadinstitute.dsde.workbench.config.LeoAuthToken
import org.broadinstitute.dsde.workbench.leonardo.{ClusterName, LeonardoConfig}
import org.broadinstitute.dsde.workbench.model.google._
import org.broadinstitute.dsde.workbench.service.RestClient
import org.openqa.selenium.WebDriver
import akka.http.scaladsl.Http
import akka.http.scaladsl.model._
import akka.http.scaladsl.server.Directives.{get => httpGet, _}

import scala.concurrent.Future
import scala.io.Source

/**
  * Leonardo API service client.
  */
object DummyClient extends RestClient with LazyLogging {

  private val url = LeonardoConfig.Leonardo.apiUrl

  def get(googleProject: GoogleProject, clusterName: ClusterName)(implicit token: AuthToken, webDriver: WebDriver): DummyClientPage = {
    val localhost = java.net.InetAddress.getLocalHost.getHostName
    val url = s"http://$localhost:9090/${googleProject.value}/${clusterName.string}/client?token=${token.value}"
    logger.info(s"Get dummy client: $url")
    new DummyClientPage(url).open
  }

  def startServer: Future[Http.ServerBinding] = {
    logger.info("Starting local server on port 9090")
    Http().bindAndHandle(route, "0.0.0.0", 9090)
  }

  def stopServer(bindingFuture: Future[Http.ServerBinding]): Future[Done] = {
    logger.info("Stopping local server")
    bindingFuture.flatMap(_.unbind())
  }

  val route: Route =
    path(Segment / Segment / "client") { (googleProject, clusterName) =>
      httpGet {
        parameter('token.as[String]) { token =>
          complete {
            logger.info(s"Serving dummy client for $googleProject/$clusterName")
            HttpEntity(ContentTypes.`text/html(UTF-8)`, getContent(GoogleProject(googleProject), ClusterName(clusterName), LeoAuthToken(token)))
          }
        }
      }
    }

  private def getContent(googleProject: GoogleProject, clusterName: ClusterName, token: LeoAuthToken) = {
    val resourceFile = ResourceFile("dummy-notebook-client.html")
    val raw = Source.fromFile(resourceFile).mkString
    val replacementMap = Map(
      "leoBaseUrl" -> url,
      "googleProject" -> googleProject.value,
      "clusterName" -> clusterName.string,
      "token" -> token.value,
      "googleClientId" -> "some-client"
    )
    replacementMap.foldLeft(raw) { case (source, (key, replacement)) =>
      source.replaceAllLiterally("$("+key+")", s"""'$replacement'""")
    }
  }
}
