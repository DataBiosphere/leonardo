package org.broadinstitute.dsde.workbench.leonardo.model

import akka.http.scaladsl.model.{StatusCode, StatusCodes}
import org.broadinstitute.dsde.workbench.model.{ErrorReport, WorkbenchException, WorkbenchExceptionWithErrorReport}
import org.broadinstitute.dsde.workbench.leonardo.errorReportSource
import org.broadinstitute.dsde.workbench.model.ErrorReport._

abstract class LeoException(
                        val message: String = null,
                        val statusCode: StatusCode = StatusCodes.InternalServerError,
                        val cause: Throwable = null) extends WorkbenchException(message) {
  def toErrorReport: ErrorReport = {
    val causeErrorReports = Option(cause).map(causes).getOrElse(Array.empty[ErrorReport])
    ErrorReport(Option(getMessage).getOrElse(""), Some(statusCode), causeErrorReports, Seq(), Some(this.getClass))
  }
}
