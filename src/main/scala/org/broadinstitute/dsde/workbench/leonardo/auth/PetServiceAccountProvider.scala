package org.broadinstitute.dsde.workbench.leonardo.auth

import com.typesafe.config.Config
import org.broadinstitute.dsde.workbench.model.{UserInfo, WorkbenchEmail}
import org.broadinstitute.dsde.workbench.model.google.GoogleProject

import scala.concurrent.{ExecutionContext, Future}

/**
  * Created by rtitle on 12/5/17.
  *
  * This is not the default ServiceAccountProvider.
  *
  * To enable, change the configuration value serviceAccounts.providerClass to
  * org.broadinstitute.dsde.workbench.leonardo.auth.PetServiceAccountProvider
  *
  * See the README for more information.
  */
class PetServiceAccountProvider(config: Config) extends SamServiceAccountProvider(config) {

  override def getClusterServiceAccount(userInfo: UserInfo, googleProject: GoogleProject)(implicit executionContext: ExecutionContext): Future[Option[WorkbenchEmail]] = {
    // Create cluster with the Google Compute Engine default service account
    Future(None)
  }

  override def getNotebookServiceAccount(userInfo: UserInfo, googleProject: GoogleProject)(implicit executionContext: ExecutionContext): Future[Option[WorkbenchEmail]] = {
    // Ask Sam for the pet service account for the user
    samDAO.getPetServiceAccountForProject(userInfo, googleProject).map(Option(_))
  }
}
