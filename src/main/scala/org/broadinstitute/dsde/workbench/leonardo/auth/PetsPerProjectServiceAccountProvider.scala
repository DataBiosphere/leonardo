package org.broadinstitute.dsde.workbench.leonardo.auth

import com.typesafe.config.Config
import org.broadinstitute.dsde.workbench.model.{UserInfo, WorkbenchEmail}
import org.broadinstitute.dsde.workbench.model.google.GoogleProject

import scala.concurrent.Future

/**
  * Created by rtitle on 12/8/17.
  */
class PetsPerProjectServiceAccountProvider(config: Config) extends SamServiceAccountProvider(config) {

  val leoServiceAccount = config.getString("leoServiceAccountEmail")

  override def getLeoServiceAccount: WorkbenchEmail = WorkbenchEmail(leoServiceAccount)

  override def getClusterServiceAccount(userInfo: UserInfo, googleProject: GoogleProject): Future[Option[WorkbenchEmail]] = {
    // Ask Sam for a pet service account for the given (user, project)
    samDAO.getPetServiceAccountForProject(userInfo, googleProject).map(Option(_))
  }

  override def getNotebookServiceAccount(userInfo: UserInfo, googleProject: GoogleProject): Future[Option[WorkbenchEmail]] = {
    Future(None)
  }

}
