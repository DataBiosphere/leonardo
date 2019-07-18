package org.broadinstitute.dsde.workbench.leonardo.notebooks


import akka.http.scaladsl.model.HttpHeader
import akka.http.scaladsl.model.headers.{Authorization, Cookie, HttpCookiePair, OAuth2BearerToken}
import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.dsde.workbench.auth.AuthToken
import org.broadinstitute.dsde.workbench.leonardo.{ClusterName, ContentItem, LeonardoConfig, NotebookContentItem}
import org.broadinstitute.dsde.workbench.model.google._
import org.broadinstitute.dsde.workbench.service.RestClient
import org.openqa.selenium.WebDriver


/**
  * Leonardo API service client.
  */
object Notebook extends RestClient with LazyLogging {

  private val url = LeonardoConfig.Leonardo.apiUrl

  def handleContentItemResponse(response: String): ContentItem = {
    mapper.readValue(response, classOf[ContentItem])
  }

  //impossible to do the handleContentResponse methods without duplication unless generics and reflection is used, which seems too complex for test code
  def handleNotebookContentResponse(response: String): NotebookContentItem = {
    mapper.readValue(response, classOf[NotebookContentItem])
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

  def getTree(googleProject: GoogleProject, clusterName: ClusterName)(implicit token: AuthToken): String = {
    val path = notebooksTreePath(googleProject, clusterName)
    logger.info(s"Get notebook tree: GET /$path")
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

  def getNotebookItem(googleProject: GoogleProject, clusterName: ClusterName, contentPath: String, includeContent: Boolean = true)(implicit token: AuthToken): NotebookContentItem = {
    val path = contentsPath(googleProject, clusterName, contentPath) + (if(includeContent) "?content=1" else "")
    logger.info(s"Get notebook contents: GET /$path")
    val cookie = Cookie(HttpCookiePair("LeoToken", token.value))

    handleNotebookContentResponse(parseResponse(getRequest(url + path, httpHeaders = List(cookie))))
  }

  def setCookie(googleProject: GoogleProject, clusterName: ClusterName)(implicit token: AuthToken, webDriver: WebDriver): String = {
    val path = notebooksBasePath(googleProject, clusterName) + "/setCookie"
    logger.info(s"Set cookie: GET /$path")
    parseResponse(getRequest(url + path, httpHeaders = List(Authorization(OAuth2BearerToken(token.value)))))
  }

  sealed trait NotebookMode extends Product with Serializable {
    def asString: String
  }

  object NotebookMode {
    final case object SafeMode extends NotebookMode {
      def asString: String = "playground"
    }

    final case object EditMode extends NotebookMode {
      def asString: String = "edit"
    }

    final case object NoMode extends NotebookMode {
      def asString: String = ""
    }

    def getModeFromString(message: String): NotebookMode = {
      message match {
        case message if message.toLowerCase().contains(SafeMode.asString) => SafeMode
        case message if message.toLowerCase().contains(EditMode.asString) => EditMode
        case _ => NoMode
      }
    }

  }
}
