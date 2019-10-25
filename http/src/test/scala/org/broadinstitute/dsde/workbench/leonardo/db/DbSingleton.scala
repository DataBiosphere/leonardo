package org.broadinstitute.dsde.workbench.leonardo.db

import org.broadinstitute.dsde.workbench.leonardo.config.LiquibaseConfig
import scala.concurrent.ExecutionContext.Implicits.global
// initialize database tables and connection pool only once
object DbSingleton {
  val ref: DbReference =
    DbReference.init(LiquibaseConfig("org/broadinstitute/dsde/workbench/leonardo/liquibase/changelog.xml", true))
}
