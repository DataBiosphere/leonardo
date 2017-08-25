package org.broadinstitute.dsde.workbench.leonardo.model

import akka.http.scaladsl.model.StatusCode
import org.broadinstitute.dsde.workbench.model.{ErrorReport, WorkbenchException, WorkbenchExceptionWithErrorReport}
import org.broadinstitute.dsde.workbench.leonardo.errorReportSource
import org.broadinstitute.dsde.workbench.model.ErrorReport._

abstract class LeoException(message: String = null) extends WorkbenchException(message) {
  def toErrorReport(statusCode: StatusCode): WorkbenchExceptionWithErrorReport = {
    new WorkbenchExceptionWithErrorReport(ErrorReport(Option(getMessage).getOrElse(""), Some(statusCode), Seq(), Seq(), Some(this.getClass)))
  }
}
