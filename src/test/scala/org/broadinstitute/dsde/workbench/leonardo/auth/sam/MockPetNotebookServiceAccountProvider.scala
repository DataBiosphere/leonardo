package org.broadinstitute.dsde.workbench.leonardo.auth.sam

import com.typesafe.config.Config
import org.broadinstitute.dsde.workbench.leonardo.dao.MockSamDAO
import org.broadinstitute.dsde.workbench.leonardo.model.ServiceAccountProvider
import org.broadinstitute.dsde.workbench.model.{UserInfo, WorkbenchEmail}
import org.broadinstitute.dsde.workbench.model.google.GoogleProject

import scala.concurrent.{ExecutionContext, Future}

/**
  * Created by rtitle on 12/4/17.
  */
class MockPetNotebookServiceAccountProvider(config: Config) extends ServiceAccountProvider(config) {
  private val mockSamClient = new MockSwaggerSamClient
  private implicit val ec = scala.concurrent.ExecutionContext.global

  override def getClusterServiceAccount(workbenchEmail: WorkbenchEmail, googleProject: GoogleProject)(implicit executionContext: ExecutionContext): Future[Option[WorkbenchEmail]] = {
    // Pretend we're using the compute engine default SA
    Future.successful(None)
  }

  override def getNotebookServiceAccount(userEmail: WorkbenchEmail, googleProject: GoogleProject)(implicit executionContext: ExecutionContext): Future[Option[WorkbenchEmail]] = {
    // Pretend we're asking Sam for the pet
    Future(Option(mockSamClient.getPetServiceAccount(userEmail, googleProject)))
  }

  override def listUsersStagingBucketReaders(userEmail: WorkbenchEmail)(implicit executionContext: ExecutionContext): Future[List[WorkbenchEmail]] = {
    Future(List.empty[WorkbenchEmail])
  }

  override def listGroupsStagingBucketReaders(userEmail: WorkbenchEmail)(implicit executionContext: ExecutionContext): Future[List[WorkbenchEmail]] = {
    Future(mockSamClient.getUserProxyFromSam(userEmail))map(List(_))
  }

  override def getAccessToken(userEmail: WorkbenchEmail, googleProject: GoogleProject): String = mockSamClient.getCachedPetAccessToken(userEmail, googleProject)
}
