package org.broadinstitute.dsde.workbench.leonardo.auth.sam

import com.typesafe.config.Config
import org.broadinstitute.dsde.workbench.leonardo.model.ServiceAccountProvider
import org.broadinstitute.dsde.workbench.model.{UserInfo, WorkbenchEmail}
import org.broadinstitute.dsde.workbench.model.google.GoogleProject

import scala.concurrent.{ExecutionContext, Future}

class PetClusterServiceAccountProvider(val config: Config) extends ServiceAccountProvider(config) with SamProvider {

  override def getClusterServiceAccount(userInfo: UserInfo, googleProject: GoogleProject)(implicit executionContext: ExecutionContext): Future[Option[WorkbenchEmail]] = {
    // Ask Sam for a pet service account for the given (user, project)
    Future {
      Option(samClient.getPetServiceAccount(userInfo, googleProject))
    }
  }

  override def getNotebookServiceAccount(userInfo: UserInfo, googleProject: GoogleProject)(implicit executionContext: ExecutionContext): Future[Option[WorkbenchEmail]] = {
    Future(None)
  }

  override def listGroupsStagingBucketReaders(userEmail: WorkbenchEmail)(implicit executionContext: ExecutionContext): Future[List[WorkbenchEmail]] = {
    Future(samClient.getUserProxyFromSam(userEmail)).map(List(_))
  }

  override def listUsersStagingBucketReaders(userEmail: WorkbenchEmail)(implicit executionContext: ExecutionContext): Future[List[WorkbenchEmail]] = {
    Future.successful(List.empty)
  }

  override def getAccessToken(userEmail: WorkbenchEmail, googleProject: GoogleProject)(implicit executionContext: ExecutionContext): Future[Option[String]] = {
    Future(Option(samClient.getCachedPetAccessToken(userEmail, googleProject)))
  }
}
