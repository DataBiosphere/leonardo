package org.broadinstitute.dsde.workbench.leonardo.auth

import com.typesafe.config.Config
import org.broadinstitute.dsde.workbench.leonardo.dao.MockSamDAO
import org.broadinstitute.dsde.workbench.leonardo.model.ServiceAccountProvider
import org.broadinstitute.dsde.workbench.model.{UserInfo, WorkbenchEmail}
import org.broadinstitute.dsde.workbench.model.google.GoogleProject

import scala.concurrent.{ExecutionContext, Future}

/**
  * Created by rtitle on 12/4/17.
  */
class MockPetServiceAccountProvider(config: Config) extends ServiceAccountProvider(config) {
  private val mockSamDAO = new MockSamDAO
  private val mockSamClient = new MockSwaggerSamClient
  private implicit val ec = scala.concurrent.ExecutionContext.global

  override def getClusterServiceAccount(userInfo: UserInfo, googleProject: GoogleProject)(implicit executionContext: ExecutionContext): Future[Option[WorkbenchEmail]] = {
    // Pretend we're using the compute engine default SA
    Future.successful(None)
  }

  override def getNotebookServiceAccount(userInfo: UserInfo, googleProject: GoogleProject)(implicit executionContext: ExecutionContext): Future[Option[WorkbenchEmail]] = {
    // Pretend we're asking Sam for the pet
    mockSamDAO.getPetServiceAccountForProject(userInfo, googleProject).map(Option(_))
  }

  override def listUsersStagingBucketReaders(userEmail: WorkbenchEmail): Future[List[WorkbenchEmail]] = {
    Future(List.empty[WorkbenchEmail])
  }

  override def listGroupsStagingBucketReaders(userEmail: WorkbenchEmail): Future[List[WorkbenchEmail]] = {
    Future(mockSamClient.getUserProxyFromSam(userEmail))
  }

}
