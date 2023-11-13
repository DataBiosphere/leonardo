package org.broadinstitute.dsde.workbench.leonardo.notebooks

import akka.http.scaladsl.model.headers._
import akka.http.scaladsl.model.{HttpHeader, Uri}
import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.dsde.workbench.auth.AuthToken
import org.broadinstitute.dsde.workbench.leonardo.{
  ContentItem,
  LeonardoConfig,
  NotebookContentItem,
  ProxyRedirectClient,
  RuntimeName
}
import org.broadinstitute.dsde.workbench.model.google._
import org.broadinstitute.dsde.workbench.service.RestClient
import org.openqa.selenium.WebDriver

import java.io.File

/**
 * Leonardo API service client.
 */
object JupyterServerClient extends RestClient with LazyLogging {

  private val url = LeonardoConfig.Leonardo.apiUrl

  private val refererUrl = ProxyRedirectClient.baseUri.renderString

  def handleContentItemResponse(response: String): ContentItem =
    mapper.readValue(response, classOf[ContentItem])

  // impossible to do the handleContentResponse methods without duplication unless generics and reflection is used, which seems too complex for test code
  def handleNotebookContentResponse(response: String): NotebookContentItem =
    mapper.readValue(response, classOf[NotebookContentItem])

  def notebooksBasePath(googleProject: GoogleProject, clusterName: RuntimeName): String =
    s"proxy/${googleProject.value}/${clusterName.asString}/jupyter"

  def notebooksTreePath(googleProject: GoogleProject, clusterName: RuntimeName): String =
    s"${notebooksBasePath(googleProject, clusterName)}/tree"

  def contentsPath(googleProject: GoogleProject, clusterName: RuntimeName, contentPath: String): String =
    s"${notebooksBasePath(googleProject, clusterName)}/api/contents/$contentPath"

  def localizePath(googleProject: GoogleProject, clusterName: RuntimeName, async: Boolean = false): String =
    s"${notebooksBasePath(googleProject, clusterName)}/api/localize${if (async) "?async=true" else ""}"

  def get(googleProject: GoogleProject, clusterName: RuntimeName)(implicit
    token: AuthToken,
    webDriver: WebDriver
  ): NotebooksListPage = {
    val path = notebooksBasePath(googleProject, clusterName)
    logger.info(s"Get notebook: GET /$path")
    new NotebooksListPage(url + path)
  }

  def createFileAtJupyterRoot(googleProject: GoogleProject, clusterName: RuntimeName, fileName: String)(implicit
    token: AuthToken
  ): File = {
    val path = contentsPath(googleProject, clusterName, fileName)
    val cookie = Cookie(HttpCookiePair("LeoToken", token.value))
    val referer = Referer(Uri(refererUrl))
    val payload: Map[String, String] = Map("path" -> fileName)

    putRequest(url + path, payload, httpHeaders = List(cookie, referer))

    new File(fileName)
  }

  def getApi(googleProject: GoogleProject, clusterName: RuntimeName)(implicit token: AuthToken): String = {
    val path = notebooksBasePath(googleProject, clusterName)
    logger.info(s"Get notebook: GET /$path")
    val referer = Referer(Uri(refererUrl))
    parseResponse(getRequest(url + path, httpHeaders = List(referer)))
  }

  def getTree(googleProject: GoogleProject, clusterName: RuntimeName)(implicit token: AuthToken): String = {
    val path = notebooksTreePath(googleProject, clusterName)
    logger.info(s"Get notebook tree: GET /$path")
    val referer = Referer(Uri(refererUrl))
    parseResponse(getRequest(url + path, httpHeaders = List(referer)))
  }

  def localize(googleProject: GoogleProject,
               clusterName: RuntimeName,
               locMap: Map[String, String],
               async: Boolean = false
  )(implicit token: AuthToken): String = {
    val path = localizePath(googleProject, clusterName, async)
    logger.info(s"Localize notebook files: POST /$path")
    val cookie = Cookie(HttpCookiePair("LeoToken", token.value))
    val referer = Referer(Uri(refererUrl))
    postRequest(url + path, locMap, httpHeaders = List(cookie, referer))
  }

  def getContentItem(googleProject: GoogleProject,
                     clusterName: RuntimeName,
                     contentPath: String,
                     includeContent: Boolean = true
  )(implicit token: AuthToken): ContentItem = {
    val path = contentsPath(googleProject, clusterName, contentPath) + (if (includeContent) "?content=1" else "")
    logger.info(s"Get notebook contents: GET /$path")
    val cookie = Cookie(HttpCookiePair("LeoToken", token.value))
    val referer = Referer(Uri(refererUrl))
    val resp = getRequest(url + path, httpHeaders = List(cookie, referer))
    handleContentItemResponse(parseResponse(resp))
  }

  def getNotebookItem(googleProject: GoogleProject,
                      clusterName: RuntimeName,
                      contentPath: String,
                      includeContent: Boolean = true
  )(implicit token: AuthToken): NotebookContentItem = {
    val path = contentsPath(googleProject, clusterName, contentPath) + (if (includeContent) "?content=1" else "")
    logger.info(s"Get notebook contents: GET /$path")
    val cookie = Cookie(HttpCookiePair("LeoToken", token.value))
    val referer = Referer(Uri(refererUrl))

    handleNotebookContentResponse(parseResponse(getRequest(url + path, httpHeaders = List(cookie, referer))))
  }

  def setCookie(googleProject: GoogleProject, clusterName: RuntimeName)(implicit token: AuthToken): String = {
    val path = notebooksBasePath(googleProject, clusterName) + "/setCookie"
    val referer = Referer(Uri(refererUrl))

    logger.info(s"Set cookie: GET /$path")
    parseResponse(getRequest(url + path, httpHeaders = List(Authorization(OAuth2BearerToken(token.value)), referer)))
  }
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

  def getModeFromString(message: String): NotebookMode =
    message match {
      case message if message.toLowerCase().contains(SafeMode.asString) => SafeMode
      case message if message.toLowerCase().contains(EditMode.asString) => EditMode
      case _                                                            => NoMode
    }

}
