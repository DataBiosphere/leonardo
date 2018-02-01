package org.broadinstitute.dsde.workbench.leonardo.auth

import com.typesafe.config.Config
import org.broadinstitute.dsde.workbench.model.{UserInfo, WorkbenchEmail}
import org.broadinstitute.dsde.workbench.model.google.GoogleProject

import scala.concurrent.{ExecutionContext, Future}

/**
  * Created by rtitle on 12/8/17.
  */
class PetsPerProjectServiceAccountProvider(config: Config) extends SamServiceAccountProvider(config) {

  override def getClusterServiceAccount(userInfo: UserInfo, googleProject: GoogleProject)(implicit executionContext: ExecutionContext): Future[Option[WorkbenchEmail]] = {
    // Ask Sam for a pet service account for the given (user, project)
    samDAO.getPetServiceAccountForProject(userInfo, googleProject).map(Option(_))
  }

  override def getNotebookServiceAccount(userInfo: UserInfo, googleProject: GoogleProject)(implicit executionContext: ExecutionContext): Future[Option[WorkbenchEmail]] = {
    Future(None)
  }

  override def listUsersStagingBucketReaders(userEmail: WorkbenchEmail)(implicit executionContext: ExecutionContext): Future[List[WorkbenchEmail]] = {
    Future(List.empty[WorkbenchEmail])
  }

  override def listGroupsStagingBucketReaders(userEmail: WorkbenchEmail)(implicit executionContext: ExecutionContext): Future[List[WorkbenchEmail]] = {
    Future(samClient.getUserProxyFromSam(userEmail)).map(List(_))
  }
}
