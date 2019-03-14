package org.broadinstitute.dsde.workbench.leonardo.lab

import akka.http.scaladsl.model.headers.{Cookie, HttpCookiePair}
import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.dsde.workbench.auth.AuthToken
import org.broadinstitute.dsde.workbench.leonardo.notebooks.Notebook.{contentsPath, getRequest, handleContentItemResponse, logger, parseResponse, url}
import org.broadinstitute.dsde.workbench.leonardo.{ClusterName, ContentItem, LeonardoConfig}
import org.broadinstitute.dsde.workbench.model.google._
import org.broadinstitute.dsde.workbench.service.RestClient
import org.openqa.selenium.WebDriver


object Lab extends RestClient with LazyLogging {

  private val url = LeonardoConfig.Leonardo.apiUrl

  def labPath(googleProject: GoogleProject, clusterName: ClusterName): String =
      s"notebooks/${googleProject.value}/${clusterName.string}/lab"

  def getApi(googleProject: GoogleProject, clusterName: ClusterName)(implicit token: AuthToken): String = {
    val path = labPath(googleProject, clusterName)
    logger.info(s"Get jupyter lab: GET /$path")
    parseResponse(getRequest(url + path))
  }

  def get(googleProject: GoogleProject, clusterName: ClusterName)(implicit token: AuthToken, webDriver: WebDriver): LabLauncherPage = {
    val path = labPath(googleProject, clusterName)
    logger.info(s"Get jupyter lab: GET /$path")
    new LabLauncherPage(url + path)
  }

  def getContentItem(googleProject: GoogleProject, clusterName: ClusterName, contentPath: String, includeContent: Boolean = true)(implicit token: AuthToken): ContentItem = {
    val path = contentsPath(googleProject, clusterName, contentPath) + (if(includeContent) "?content=1" else "")
    logger.info(s"Get lab notebook contents: GET /$path")
    val cookie = Cookie(HttpCookiePair("LeoToken", token.value))
    handleContentItemResponse(parseResponse(getRequest(url + path, httpHeaders = List(cookie))))
  }
}
