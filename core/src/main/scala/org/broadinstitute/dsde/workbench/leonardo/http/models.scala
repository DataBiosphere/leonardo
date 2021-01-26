package org.broadinstitute.dsde.workbench.leonardo.http

import org.broadinstitute.dsde.workbench.model.{ErrorReport, ErrorReportSource, WorkbenchException}
import akka.http.scaladsl.model.{StatusCode, StatusCodes}

class LeoException(val message: String = null,
                   val statusCode: StatusCode = StatusCodes.InternalServerError,
                   val cause: Throwable = null)
    extends WorkbenchException(message, cause) {
  override def getMessage: String = if (message != null) message else super.getMessage

  def toErrorReport(implicit es: ErrorReportSource): ErrorReport =
    ErrorReport(Option(getMessage).getOrElse(""), Some(statusCode), Seq(), Seq(), Some(this.getClass))
}
