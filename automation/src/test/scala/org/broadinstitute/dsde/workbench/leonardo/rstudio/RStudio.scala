package org.broadinstitute.dsde.workbench.leonardo.rstudio

import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.dsde.workbench.auth.AuthToken
import org.broadinstitute.dsde.workbench.leonardo.{ClusterName, LeonardoConfig}
import org.broadinstitute.dsde.workbench.model.google._
import org.broadinstitute.dsde.workbench.service.RestClient

/**
  * Leonardo RStudio API service client.
  */
object RStudio extends RestClient with LazyLogging {

  private val url = LeonardoConfig.Leonardo.apiUrl

  def rstudioPath(googleProject: GoogleProject, clusterName: ClusterName): String =
    s"proxy/${googleProject.value}/${clusterName.string}/rstudio"

  def getApi(googleProject: GoogleProject, clusterName: ClusterName)(implicit token: AuthToken): String = {
    val path = rstudioPath(googleProject, clusterName)
    logger.info(s"Get rstudio: GET /$path")
    parseResponse(getRequest(url + path))
  }
}
