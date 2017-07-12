package org.broadinstitute.dsde.workbench

import org.broadinstitute.dsde.workbench.leonardo.model.ErrorReportSource

package object leonardo {
  implicit val errorReportSource = ErrorReportSource("leonardo")
}
