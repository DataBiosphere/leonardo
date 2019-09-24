package org.broadinstitute.dsde.workbench.leonardo.dao

import org.broadinstitute.dsde.workbench.model.ErrorReportSource
import org.broadinstitute.dsde.workbench.util.health.StatusCheckResponse

import scala.concurrent.Future

/**
  * Created by rtitle on 10/16/17.
  */
trait SamDAO {
  implicit val errorReportSource = ErrorReportSource("sam")

  def getStatus(): Future[StatusCheckResponse]
}
