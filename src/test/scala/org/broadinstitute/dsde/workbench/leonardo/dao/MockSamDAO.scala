package org.broadinstitute.dsde.workbench.leonardo.dao

import org.broadinstitute.dsde.workbench.model.{WorkbenchEmail, UserInfo}
import org.broadinstitute.dsde.workbench.util.health.SubsystemStatus

import scala.concurrent.Future

/**
  * Created by rtitle on 10/16/17.
  */
class MockSamDAO(ok: Boolean = true) extends SamDAO {
  val serviceAccount = WorkbenchEmail("pet-1234567890@test-project.iam.gserviceaccount.com")

  override def getStatus(): Future[SubsystemStatus] = {
    if (ok) Future.successful(SubsystemStatus(true, None))
    else Future.successful(SubsystemStatus(false, None))
  }

  override def getPetServiceAccount(userInfo: UserInfo): Future[WorkbenchEmail] = {
    Future.successful(serviceAccount)
  }
}
