package org.broadinstitute.dsde.workbench.leonardo.notebooks

import org.broadinstitute.dsde.workbench.service.RestClient
import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.dsde.workbench.leonardo._
import org.broadinstitute.dsde.workbench.model.google.GoogleProject
import org.broadinstitute.dsde.workbench.model.google._
import org.broadinstitute.dsde.workbench.auth.AuthToken
import org.broadinstitute.dsde.workbench.config.LeoAuthToken


/**
  * Leonardo API service client.
  */
object Welder extends RestClient with LazyLogging {

  private val url = LeonardoConfig.Leonardo.apiUrl

  def welderBasePath(googleProject: GoogleProject, clusterName: ClusterName): String = {
//    s"${url}/proxy/${googleProject.value}/${clusterName.string}/welder"
    s"http://10.1.3.12:8080"
  }

  def getWelderStatus(cluster: Cluster)(implicit token: AuthToken): String = {
    val path = welderBasePath(cluster.googleProject, cluster.clusterName)
    println("attempting to get status")
    logger.info(s"Get welder status: GET $path/status")

    val resp = parseResponse(getRequest(path + "/status"))
    println(resp)
    resp
  }

}