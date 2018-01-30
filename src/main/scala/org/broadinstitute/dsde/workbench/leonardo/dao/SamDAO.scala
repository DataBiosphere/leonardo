package org.broadinstitute.dsde.workbench.leonardo.dao

import org.broadinstitute.dsde.workbench.model.google.GoogleProject
import org.broadinstitute.dsde.workbench.model.{ErrorReportSource, UserInfo, WorkbenchEmail}
import org.broadinstitute.dsde.workbench.util.health.StatusCheckResponse

import scala.concurrent.Future

/**
  * Created by rtitle on 10/16/17.
  */
trait SamDAO {
  implicit val errorReportSource = ErrorReportSource("sam")

  def getStatus(): Future[StatusCheckResponse]

  def getPetServiceAccountForProject(userInfo: UserInfo, googleProject: GoogleProject): Future[WorkbenchEmail]
}
