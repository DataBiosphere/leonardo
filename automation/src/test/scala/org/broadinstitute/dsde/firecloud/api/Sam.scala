package org.broadinstitute.dsde.firecloud.api

import akka.http.scaladsl.model.StatusCodes
import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.dsde.firecloud.config.FireCloudConfig
import org.broadinstitute.dsde.workbench.api.WorkbenchClient
import org.broadinstitute.dsde.workbench.config.AuthToken

/**
  * Sam API service client. This should only be used when Orchestration does
  * not provide a required endpoint. This should primarily be used for admin
  * functions.
  */
object Sam extends WorkbenchClient with LazyLogging {

  private val url = FireCloudConfig.FireCloud.samApiUrl

  object admin {

    def deleteUser(subjectId: String)(implicit token: AuthToken): Unit = {
      logger.info(s"Deleting user: $subjectId")
      deleteRequest(url + s"api/admin/user/$subjectId")
    }

    def doesUserExist(subjectId: String)(implicit token: AuthToken): Option[Boolean] = {
      getRequest(url + s"api/admin/user/$subjectId").status match {
        case StatusCodes.OK => Option(true)
        case StatusCodes.NotFound => Option(false)
        case _ => None
      }
    }
  }
}
