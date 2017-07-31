package org.broadinstitute.dsde.workbench

import org.broadinstitute.dsde.workbench.model.ErrorReportSource

package object leonardo {
  implicit val errorReportSource = ErrorReportSource("leonardo")
}
