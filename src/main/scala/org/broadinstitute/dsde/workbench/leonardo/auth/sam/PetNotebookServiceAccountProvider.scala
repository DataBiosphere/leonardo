package org.broadinstitute.dsde.workbench.leonardo.auth.sam

import com.typesafe.config.Config
import org.broadinstitute.dsde.workbench.leonardo.model.ServiceAccountProvider
import org.broadinstitute.dsde.workbench.model.{UserInfo, WorkbenchEmail}
import org.broadinstitute.dsde.workbench.model.google.GoogleProject

import scala.concurrent._

/**
  * Created by rtitle on 12/5/17.
  *
  * This is not the default ServiceAccountProvider.
  *
  * To enable, change the configuration value serviceAccounts.providerClass to
  * org.broadinstitute.dsde.workbench.leonardo.auth.PetNotebookServiceAccountProvider
  *
  * See the README for more information.
  */
class PetNotebookServiceAccountProvider(val config: Config) extends ServiceAccountProvider(config) with SamProvider {

  override def getClusterServiceAccount(userInfo: UserInfo, googleProject: GoogleProject)(implicit executionContext: ExecutionContext): Future[Option[WorkbenchEmail]] = {
    // Create cluster with the Google Compute Engine default service account
    Future(None)
  }

  override def getNotebookServiceAccount(userInfo: UserInfo, googleProject: GoogleProject)(implicit executionContext: ExecutionContext): Future[Option[WorkbenchEmail]] = {
    // Ask Sam for a pet service account for the given (user, project)
    Future {
      Option(blocking(samClient.getPetServiceAccount(userInfo, googleProject)))
    }
  }

  override def listGroupsStagingBucketReaders(userEmail: WorkbenchEmail)(implicit executionContext: ExecutionContext): Future[List[WorkbenchEmail]] = {
    Future(samClient.getUserProxyFromSam(userEmail)).map(List(_))
  }

  override def listUsersStagingBucketReaders(userEmail: WorkbenchEmail)(implicit executionContext: ExecutionContext): Future[List[WorkbenchEmail]] = {
    Future.successful(List.empty)
  }

  override def getAccessToken(userEmail: WorkbenchEmail, googleProject: GoogleProject)(implicit executionContext: ExecutionContext): Future[Option[String]] = {
    Future(Option(blocking(samClient.getCachedPetAccessToken(userEmail, googleProject))))
  }
}
