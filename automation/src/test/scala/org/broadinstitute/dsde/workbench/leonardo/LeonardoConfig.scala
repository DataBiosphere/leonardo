package org.broadinstitute.dsde.workbench.leonardo

import org.broadinstitute.dsde.workbench.config.WorkbenchConfig

object LeonardoConfig extends WorkbenchConfig {
  private val leonardo = config.getConfig("leonardo")

  object Leonardo {
    val apiUrl: String = leonardo.getString("apiUrl")
    val notebooksServiceAccountEmail: String = leonardo.getString("notebooksServiceAccountEmail")
  }
}
