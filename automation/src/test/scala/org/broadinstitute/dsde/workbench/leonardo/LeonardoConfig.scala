package org.broadinstitute.dsde.workbench.leonardo

import org.broadinstitute.dsde.workbench.config.CommonConfig

object LeonardoConfig extends CommonConfig {
  private val leonardo = config.getConfig("leonardo")

  object Leonardo {
    val apiUrl: String = leonardo.getString("apiUrl")
    val notebooksServiceAccountEmail: String = leonardo.getString("notebooksServiceAccountEmail")
    val notebooksCanaryProject: String = leonardo.getString("notebooksCanaryProject")
  }

  // for qaEmail and pathToQAPem
  object GCS extends CommonGCS

  // for NotebooksWhitelisted
  object Users extends CommonUsers
}
