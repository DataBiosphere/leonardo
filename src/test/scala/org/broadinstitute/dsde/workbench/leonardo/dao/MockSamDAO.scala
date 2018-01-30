package org.broadinstitute.dsde.workbench.leonardo.dao

import org.broadinstitute.dsde.workbench.model.google.GoogleProject
import org.broadinstitute.dsde.workbench.model.{UserInfo, WorkbenchEmail}
import org.broadinstitute.dsde.workbench.util.health.Subsystems.OpenDJ
import org.broadinstitute.dsde.workbench.util.health.{StatusCheckResponse, SubsystemStatus}

import scala.concurrent.Future

/**
  * Created by rtitle on 10/16/17.
  */
class MockSamDAO(ok: Boolean = true) extends SamDAO {
  val serviceAccount = WorkbenchEmail("pet-1234567890@test-project.iam.gserviceaccount.com")

  override def getStatus(): Future[StatusCheckResponse] = {
    if (ok) Future.successful(StatusCheckResponse(true, Map.empty))
    else Future.successful(StatusCheckResponse(false, Map(OpenDJ -> SubsystemStatus(false, Some(List("OpenDJ is down. Panic!"))))))
  }

  override def getPetServiceAccountForProject(userInfo: UserInfo, googleProject: GoogleProject): Future[WorkbenchEmail] = {
    Future.successful(serviceAccount)
  }
}
