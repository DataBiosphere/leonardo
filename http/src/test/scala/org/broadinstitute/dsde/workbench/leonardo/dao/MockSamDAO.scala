package org.broadinstitute.dsde.workbench.leonardo.dao

import org.broadinstitute.dsde.workbench.util.health.Subsystems.OpenDJ
import org.broadinstitute.dsde.workbench.util.health.{StatusCheckResponse, SubsystemStatus}

import scala.concurrent.Future

/**
  * Created by rtitle on 10/16/17.
  */
class MockSamDAO(ok: Boolean = true) extends SamDAO {

  override def getStatus(): Future[StatusCheckResponse] = {
    if (ok) Future.successful(StatusCheckResponse(true, Map.empty))
    else Future.successful(StatusCheckResponse(false, Map(OpenDJ -> SubsystemStatus(false, Some(List("OpenDJ is down. Panic!"))))))
  }

}
