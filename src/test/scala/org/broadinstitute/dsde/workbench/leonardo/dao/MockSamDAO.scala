package org.broadinstitute.dsde.workbench.leonardo.dao

import org.broadinstitute.dsde.workbench.leonardo.model.UserInfo
import org.broadinstitute.dsde.workbench.model.WorkbenchUserServiceAccountEmail
import org.broadinstitute.dsde.workbench.util.health.SubsystemStatus

import scala.concurrent.Future

/**
  * Created by rtitle on 10/16/17.
  */
class MockSamDAO extends SamDAO {
  val serviceAccount = WorkbenchUserServiceAccountEmail("pet-1234567890@test-project.iam.gserviceaccount.com")

  override def getStatus(): Future[SubsystemStatus] = {
    Future.successful(SubsystemStatus(true, None))
  }

  override def getPetServiceAccount(userInfo: UserInfo): Future[WorkbenchUserServiceAccountEmail] = {
    Future.successful(serviceAccount)
  }
}
