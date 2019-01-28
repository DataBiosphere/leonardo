package org.broadinstitute.dsde.workbench.leonardo.lab

import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.dsde.workbench.auth.AuthToken
import org.broadinstitute.dsde.workbench.leonardo.{ClusterName, LeonardoConfig}
import org.broadinstitute.dsde.workbench.model.google._
import org.broadinstitute.dsde.workbench.service.RestClient
import org.openqa.selenium.WebDriver


object Lab extends RestClient with LazyLogging {

  private val url = LeonardoConfig.Leonardo.apiUrl

  def labPath(googleProject: GoogleProject, clusterName: ClusterName): String =
      s"notebooks/${googleProject.value}/${clusterName.string}/lab"

    def getApi(googleProject: GoogleProject, clusterName: ClusterName)(implicit token: AuthToken): String = {
      val path = labPath(googleProject, clusterName)
      logger.info(s"Get notebook: GET /$path")
      parseResponse(getRequest(url + path))
    }

    def get(googleProject: GoogleProject, clusterName: ClusterName)(implicit token: AuthToken, webDriver: WebDriver): LabLauncherPage = {
      val path = labPath(googleProject, clusterName)
      logger.info(s"Get notebook: GET /$path")
      new LabLauncherPage(url + path)
    }
  }
