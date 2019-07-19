package org.broadinstitute.dsde.workbench.leonardo

import org.broadinstitute.dsde.workbench.config.CommonConfig

object LeonardoConfig extends CommonConfig {
  private val leonardo = config.getConfig("leonardo")
  private val gcs = config.getConfig("gcs")

  object Leonardo {
    val apiUrl: String = leonardo.getString("apiUrl")
    val notebooksServiceAccountEmail: String = leonardo.getString("notebooksServiceAccountEmail")
  }

  // for qaEmail and pathToQAPem and pathToQAJson
  object GCS extends CommonGCS {
    val pathToQAJson = gcs.getString("qaJsonFile")
  }

  // for NotebooksWhitelisted
  object Users extends CommonUsers
}
