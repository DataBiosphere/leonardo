package org.broadinstitute.dsde.workbench.leonardo

import org.broadinstitute.dsde.workbench.config.Config

object LeonardoConfig extends Config {
  private val leonardo = Config.config.getConfig("leonardo")

  object Leonardo {
    val apiUrl: String = leonardo.getString("apiUrl")
    val notebooksServiceAccountEmail: String = leonardo.getString("notebooksServiceAccountEmail")
  }
}
