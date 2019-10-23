package org.broadinstitute.dsde.workbench.leonardo

import org.broadinstitute.dsde.workbench.config.CommonConfig

object LeonardoConfig extends CommonConfig {
  private val leonardo = config.getConfig("leonardo")
  private val gcs = config.getConfig("gcs")

  object Leonardo {
    val apiUrl: String = leonardo.getString("apiUrl")
    val notebooksServiceAccountEmail: String = leonardo.getString("notebooksServiceAccountEmail")
    val rImageUrl: String = leonardo.getString("rImageUrl")
    val pythonImageUrl: String = leonardo.getString("pythonImageUrl")
    val hailImageUrl: String = leonardo.getString("hailImageUrl")
    val gatkImageUrl: String = leonardo.getString("gatkImageUrl")
    val baseImageUrl: String = leonardo.getString("baseImageUrl")
    val oldWelderDockerImage: String = leonardo.getString("oldWelderImage")
    val curWelderDockerImage: String = leonardo.getString("currentWelderImage")
  }

  // for qaEmail and pathToQAPem and pathToQAJson
  object GCS extends CommonGCS {
    val pathToQAJson = gcs.getString("qaJsonFile")
  }

  // for NotebooksWhitelisted
  object Users extends CommonUsers
}
