package org.broadinstitute.dsde.workbench.leonardo.auth

import com.typesafe.config.Config
import org.broadinstitute.dsde.workbench.leonardo.model.ServiceAccountProvider
import org.broadinstitute.dsde.workbench.model.WorkbenchEmail
import org.broadinstitute.dsde.workbench.model.google.GoogleProject

import scala.concurrent.{ExecutionContext, Future}

/**
  * A ServiceAccountProvider which uses all defaults.
  */
class DefaultServiceAccountProvider(config: Config) extends ServiceAccountProvider(config) {

  override def getClusterServiceAccount(userEmail: WorkbenchEmail, googleProject: GoogleProject)(implicit executionContext: ExecutionContext): Future[Option[WorkbenchEmail]] = {
    // Create clusters with the Compute Engine default service account
    Future.successful(None)
  }

  override def getNotebookServiceAccount(userEmail: WorkbenchEmail, googleProject: GoogleProject)(implicit executionContext: ExecutionContext): Future[Option[WorkbenchEmail]] = {
    // Don't localize any credentials to the notebook server
    Future.successful(None)
  }

  override def listGroupsStagingBucketReaders(userEmail: WorkbenchEmail)(implicit executionContext: ExecutionContext): Future[List[WorkbenchEmail]] = {
    Future.successful(List.empty)
  }

  override def listUsersStagingBucketReaders(userEmail: WorkbenchEmail)(implicit executionContext: ExecutionContext): Future[List[WorkbenchEmail]] = {
    Future.successful(List.empty)
  }

  override def getAccessToken(userEmail: WorkbenchEmail, googleProject: GoogleProject): String = ""
}
