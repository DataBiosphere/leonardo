package org.broadinstitute.dsde.workbench

import org.broadinstitute.dsde.workbench.leonardo.db.DBIOOps
import org.broadinstitute.dsde.workbench.model.ErrorReportSource
import slick.dbio.DBIO

package object leonardo {
  implicit val errorReportSource = ErrorReportSource("leonardo")
  final val autoPauseOffValue = 0
  implicit def dbioToIO[A](dbio: DBIO[A]): DBIOOps[A] = new DBIOOps(dbio)
}
