package org.broadinstitute.dsde.workbench.leonardo.dao

import org.broadinstitute.dsde.workbench.leonardo.model.UserInfo
import org.broadinstitute.dsde.workbench.model.{ErrorReportSource, WorkbenchUserServiceAccountEmail}
import org.broadinstitute.dsde.workbench.util.health.SubsystemStatus

import scala.concurrent.Future

/**
  * Created by rtitle on 10/16/17.
  */
trait SamDAO {
  implicit val errorReportSource = ErrorReportSource("sam")

  def getStatus(): Future[SubsystemStatus]

  def getPetServiceAccount(userInfo: UserInfo): Future[WorkbenchUserServiceAccountEmail]
}
