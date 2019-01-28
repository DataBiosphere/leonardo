package org.broadinstitute.dsde.workbench.leonardo.notebooks

import akka.Done
import akka.http.scaladsl.model.HttpHeader
import akka.http.scaladsl.model.headers.{Authorization, Cookie, HttpCookiePair, OAuth2BearerToken}
import akka.http.scaladsl.server.Route
import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.dsde.workbench.ResourceFile
import org.broadinstitute.dsde.workbench.auth.AuthToken
import org.broadinstitute.dsde.workbench.config.LeoAuthToken
import org.broadinstitute.dsde.workbench.leonardo.{ClusterName, LeonardoConfig}
import org.broadinstitute.dsde.workbench.model.google._
import org.broadinstitute.dsde.workbench.service.RestClient
import org.broadinstitute.dsde.workbench.leonardo.ContentItem
import org.openqa.selenium.WebDriver

import scala.concurrent.Future
import scala.io.Source

/**
  * Leonardo API service client.
  */
object Notebook extends RestClient with LazyLogging {

  private val url = LeonardoConfig.Leonardo.apiUrl


    def handleContentItemResponse(response: String): ContentItem = {
      mapper.readValue(response, classOf[ContentItem])
    }

    def notebooksBasePath(googleProject: GoogleProject, clusterName: ClusterName): String =
      s"notebooks/${googleProject.value}/${clusterName.string}"

    def notebooksTreePath(googleProject: GoogleProject, clusterName: ClusterName): String =
      s"${notebooksBasePath(googleProject, clusterName)}/tree"

    def contentsPath(googleProject: GoogleProject, clusterName: ClusterName, contentPath: String): String =
      s"${notebooksBasePath(googleProject, clusterName)}/api/contents/$contentPath"

    def localizePath(googleProject: GoogleProject, clusterName: ClusterName, async: Boolean = false): String =
      s"${notebooksBasePath(googleProject, clusterName)}/api/localize${if (async) "?async=true" else ""}"

    def get(googleProject: GoogleProject, clusterName: ClusterName)(implicit token: AuthToken, webDriver: WebDriver): NotebooksListPage = {
      val path = notebooksBasePath(googleProject, clusterName)
      logger.info(s"Get notebook: GET /$path")
      new NotebooksListPage(url + path)
    }

    def getApi(googleProject: GoogleProject, clusterName: ClusterName)(implicit token: AuthToken): String = {
      val path = notebooksBasePath(googleProject, clusterName)
      logger.info(s"Get notebook: GET /$path")
      parseResponse(getRequest(url + path))
    }

    def getApiHeaders(googleProject: GoogleProject, clusterName: ClusterName)(implicit token: AuthToken): Seq[HttpHeader] = {
      val path = notebooksTreePath(googleProject, clusterName)
      logger.info(s"Get notebook: GET /$path")
      getRequest(url + path).headers
    }

    def localize(googleProject: GoogleProject, clusterName: ClusterName, locMap: Map[String, String], async: Boolean = false)(implicit token: AuthToken): String = {
      val path = localizePath(googleProject, clusterName, async)
      logger.info(s"Localize notebook files: POST /$path")
      val cookie = Cookie(HttpCookiePair("LeoToken", token.value))
      postRequest(url + path, locMap, httpHeaders = List(cookie))
    }

    def getContentItem(googleProject: GoogleProject, clusterName: ClusterName, contentPath: String, includeContent: Boolean = true)(implicit token: AuthToken): ContentItem = {
      val path = contentsPath(googleProject, clusterName, contentPath) + (if(includeContent) "?content=1" else "")
      logger.info(s"Get notebook contents: GET /$path")
      val cookie = Cookie(HttpCookiePair("LeoToken", token.value))
      handleContentItemResponse(parseResponse(getRequest(url + path, httpHeaders = List(cookie))))
    }

    def setCookie(googleProject: GoogleProject, clusterName: ClusterName)(implicit token: AuthToken, webDriver: WebDriver): String = {
      val path = notebooksBasePath(googleProject, clusterName) + "/setCookie"
      logger.info(s"Set cookie: GET /$path")
      parseResponse(getRequest(url + path, httpHeaders = List(Authorization(OAuth2BearerToken(token.value)))))

    }


  object dummyClient {
    import akka.http.scaladsl.Http
    import akka.http.scaladsl.model._
    import akka.http.scaladsl.server.Directives.{get => httpGet, _}

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
}
