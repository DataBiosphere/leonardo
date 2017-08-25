package org.broadinstitute.dsde.workbench.leonardo.model

import akka.http.scaladsl.model.StatusCode
import org.broadinstitute.dsde.workbench.model.{ErrorReport, WorkbenchException, WorkbenchExceptionWithErrorReport}
import org.broadinstitute.dsde.workbench.leonardo.errorReportSource
import org.broadinstitute.dsde.workbench.model.ErrorReport._

abstract class LeoException(message: String = null, cause: Throwable = null) extends WorkbenchException(message, cause) {
  def toErrorReport(statusCode: StatusCode): WorkbenchExceptionWithErrorReport = {
    Option(cause) match {
      case Some(c) => new WorkbenchExceptionWithErrorReport(ErrorReport(Option(getMessage).getOrElse(""), Some(statusCode), ErrorReport.causes(cause), Seq(), Some(this.getClass)))
      case None => new WorkbenchExceptionWithErrorReport(ErrorReport(Option(getMessage).getOrElse(""), Some(statusCode), Seq(), Seq(), Some(this.getClass)))
    }
  }
}
