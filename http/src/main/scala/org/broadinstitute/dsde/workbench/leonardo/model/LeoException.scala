package org.broadinstitute.dsde.workbench.leonardo.model

import akka.http.scaladsl.model.{StatusCode, StatusCodes}
import org.broadinstitute.dsde.workbench.model.{ErrorReport, WorkbenchException}
import org.broadinstitute.dsde.workbench.leonardo.http.errorReportSource

class LeoException(val message: String = null,
                   val statusCode: StatusCode = StatusCodes.InternalServerError,
                   val cause: Throwable = null)
    extends WorkbenchException(message, cause) {
  override def getMessage: String = if (message != null) message else super.getMessage

//  override def getCause = cause

  def toErrorReport: ErrorReport =
    ErrorReport(Option(getMessage).getOrElse(""), Some(statusCode), Seq(), Seq(), Some(this.getClass))
}
