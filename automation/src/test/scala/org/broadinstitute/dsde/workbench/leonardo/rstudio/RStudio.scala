package org.broadinstitute.dsde.workbench.leonardo.rstudio

import akka.http.scaladsl.model.Uri
import akka.http.scaladsl.model.headers.Referer
import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.dsde.workbench.auth.AuthToken
import org.broadinstitute.dsde.workbench.leonardo.{LeonardoConfig, ProxyRedirectClient, RuntimeName}
import org.broadinstitute.dsde.workbench.model.google._
import org.broadinstitute.dsde.workbench.service.RestClient
import org.openqa.selenium.WebDriver

/**
 * Leonardo RStudio API service client.
 */
object RStudio extends RestClient with LazyLogging {

  private val url = LeonardoConfig.Leonardo.apiUrl

  private val refererUrl =
    ProxyRedirectClient.baseUri.renderString

  private def rstudioPath(googleProject: GoogleProject, clusterName: RuntimeName): String =
    s"proxy/${googleProject.value}/${clusterName.asString}/rstudio/"

  def get(googleProject: GoogleProject, clusterName: RuntimeName)(implicit token: AuthToken,
                                                                  webDriver: WebDriver): RStudioPage = {
    val path = rstudioPath(googleProject, clusterName)
    logger.info(s"Get rstudio: GET /$path")
    new RStudioPage(url + path)
  }

  def getApi(googleProject: GoogleProject, clusterName: RuntimeName)(implicit token: AuthToken): String = {
    val path = rstudioPath(googleProject, clusterName)
    val referer = Referer(Uri(refererUrl))
    logger.info(s"Get rstudio: GET /$path")
    parseResponse(getRequest(url + path, httpHeaders = List(referer)))
  }
}
