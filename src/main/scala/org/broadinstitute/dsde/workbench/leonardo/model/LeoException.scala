package org.broadinstitute.dsde.workbench.leonardo.model

import akka.http.scaladsl.model.{StatusCode, StatusCodes}
import org.broadinstitute.dsde.workbench.model.{ErrorReport, WorkbenchException, WorkbenchExceptionWithErrorReport}
import org.broadinstitute.dsde.workbench.leonardo.errorReportSource
import org.broadinstitute.dsde.workbench.model.ErrorReport._

import scala.concurrent.Future

abstract class LeoException(
                        val message: String = null,
                        val statusCode: StatusCode = StatusCodes.InternalServerError,
                        val cause: Throwable = null) extends WorkbenchException(message) {
  def toErrorReport: ErrorReport = {
    ErrorReport(Option(getMessage).getOrElse(""), Some(statusCode), Seq(), Seq(), Some(this.getClass))
  }
}