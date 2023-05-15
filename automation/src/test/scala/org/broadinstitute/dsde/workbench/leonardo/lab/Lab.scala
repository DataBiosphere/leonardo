package org.broadinstitute.dsde.workbench.leonardo.lab

import akka.http.scaladsl.model.Uri
import akka.http.scaladsl.model.headers.{Cookie, HttpCookiePair, Referer}
import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.dsde.workbench.auth.AuthToken
import org.broadinstitute.dsde.workbench.leonardo.notebooks.Notebook
import org.broadinstitute.dsde.workbench.leonardo.{ContentItem, LeonardoConfig, ProxyRedirectClient, RuntimeName}
import org.broadinstitute.dsde.workbench.model.google._
import org.broadinstitute.dsde.workbench.service.RestClient
import org.openqa.selenium.WebDriver

// TODO here too?
object Lab extends RestClient with LazyLogging {

  private val url = LeonardoConfig.Leonardo.apiUrl

  private val refererUrl =
    ProxyRedirectClient.baseUri.renderString

  def labPath(googleProject: GoogleProject, clusterName: RuntimeName): String =
    s"notebooks/${googleProject.value}/${clusterName.asString}/lab"

  def getApi(googleProject: GoogleProject, clusterName: RuntimeName)(implicit token: AuthToken): String = {
    val path = labPath(googleProject, clusterName)
    val referer = Referer(Uri(refererUrl))
    logger.info(s"Get jupyter lab: GET /$path")
    parseResponse(getRequest(url + path, httpHeaders = List(referer)))
  }

  def get(googleProject: GoogleProject, clusterName: RuntimeName)(implicit
    token: AuthToken,
    webDriver: WebDriver
  ): LabLauncherPage = {
    val path = labPath(googleProject, clusterName)
    logger.info(s"Get jupyter lab: GET /$path")
    new LabLauncherPage(url + path)
  }

  def getContentItem(googleProject: GoogleProject,
                     clusterName: RuntimeName,
                     contentPath: String,
                     includeContent: Boolean = true
  )(implicit token: AuthToken): ContentItem = {
    val path =
      Notebook.contentsPath(googleProject, clusterName, contentPath) + (if (includeContent) "?content=1" else "")
    logger.info(s"Get lab notebook contents: GET /$path")
    val cookie = Cookie(HttpCookiePair("LeoToken", token.value))
    val referer = Referer(Uri(refererUrl))
    Notebook.handleContentItemResponse(parseResponse(getRequest(url + path, httpHeaders = List(cookie, referer))))
  }
}
