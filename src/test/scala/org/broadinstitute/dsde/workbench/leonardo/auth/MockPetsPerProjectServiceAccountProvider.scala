package org.broadinstitute.dsde.workbench.leonardo.auth

import com.typesafe.config.Config
import org.broadinstitute.dsde.workbench.leonardo.dao.MockSamDAO
import org.broadinstitute.dsde.workbench.leonardo.model.ServiceAccountProvider
import org.broadinstitute.dsde.workbench.model.{UserInfo, WorkbenchEmail}
import org.broadinstitute.dsde.workbench.model.google.GoogleProject

import scala.concurrent.{ExecutionContext, Future}

/**
  * Created by rtitle on 12/11/17.
  */
class MockPetsPerProjectServiceAccountProvider(config: Config) extends ServiceAccountProvider(config) {
  private val mockSamDAO = new MockSamDAO
  private val mockSwaggerSamClient = new MockSwaggerSamClient
  private implicit val ec = scala.concurrent.ExecutionContext.global

  override def getClusterServiceAccount(userInfo: UserInfo, googleProject: GoogleProject)(implicit executionContext: ExecutionContext): Future[Option[WorkbenchEmail]] = {
    // Pretend we're asking Sam for the pet
    mockSamDAO.getPetServiceAccountForProject(userInfo, googleProject).map(Option(_))
  }

  override def getNotebookServiceAccount(userInfo: UserInfo, googleProject: GoogleProject)(implicit executionContext: ExecutionContext): Future[Option[WorkbenchEmail]] = {
    Future(None)
  }

  def listGroupsStagingBucketReaders(userEmail: WorkbenchEmail)(implicit executionContext: ExecutionContext): scala.concurrent.Future[List[WorkbenchEmail]] = {
    Future.successful(List.empty[WorkbenchEmail])
  }

  def listUsersStagingBucketReaders(userEmail: WorkbenchEmail)(implicit executionContext: ExecutionContext): scala.concurrent.Future[List[WorkbenchEmail]] = {
    Future(mockSwaggerSamClient.getUserProxyFromSam(userEmail))
  }
}
