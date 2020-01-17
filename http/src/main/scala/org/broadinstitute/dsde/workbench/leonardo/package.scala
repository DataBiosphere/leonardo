package org.broadinstitute.dsde.workbench.leonardo

import org.broadinstitute.dsde.workbench.leonardo.db.DBIOOps
import org.broadinstitute.dsde.workbench.model.ErrorReportSource
import slick.dbio.DBIO

package object http {
  implicit val errorReportSource = ErrorReportSource("leonardo")
  implicit def dbioToIO[A](dbio: DBIO[A]): DBIOOps[A] = new DBIOOps(dbio)
}
