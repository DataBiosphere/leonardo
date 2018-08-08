package org.broadinstitute.dsde.workbench.leonardo

import akka.http.scaladsl.model.{StatusCode, StatusCodes}
import org.broadinstitute.dsde.workbench.model.{ErrorReport, ErrorReportSource, WorkbenchException}

abstract class LeoException(
                        val message: String = null,
                        val statusCode: StatusCode = StatusCodes.InternalServerError,
                        val cause: Throwable = null) extends WorkbenchException(message) {

  implicit val errorReportSource = ErrorReportSource("test")

  def toErrorReport: ErrorReport = {
    ErrorReport(Option(getMessage).getOrElse(""), Some(statusCode), Seq(), Seq(), Some(this.getClass))
  }
}
