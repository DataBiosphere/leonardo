package org.broadinstitute.dsde.workbench.leonardo

import com.google.pubsub.v1.ProjectTopicName
import org.broadinstitute.dsde.workbench.config.CommonConfig
import org.broadinstitute.dsde.workbench.google2.PublisherConfig
import org.broadinstitute.dsde.workbench.google2.GoogleTopicAdminInterpreter

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
    val aouImageUrl: String = leonardo.getString("aouImageUrl")
    val baseImageUrl: String = leonardo.getString("baseImageUrl")
    val oldWelderHash: String = leonardo.getString("oldWelderHash")
    val curWelderHash: String = leonardo.getString("curWelderHash")
    val oldGcrWelderDockerImage: ContainerImage =
      ContainerImage(leonardo.getString("gcrWelderUri") + ":" + oldWelderHash, ContainerRegistry.GCR)
    val oldDockerHubWelderDockerImage: ContainerImage =
      ContainerImage(leonardo.getString("dockerHubWelderUri") + ":" + oldWelderHash, ContainerRegistry.DockerHub)
    val curGcrWelderDockerImage: String = leonardo.getString("gcrWelderUri") + ":" + curWelderHash
    val curDockerHubWelderDockerImage: String = leonardo.getString("dockerHubWelderUri") + ":" + curWelderHash
    val bioconductorImageUrl: String = leonardo.getString("bioconductorImageUrl")
    val rstudioBaseImageUrl = ContainerImage(leonardo.getString("rstudioBaseImageUrl"), ContainerRegistry.GCR)

    private val topic = ProjectTopicName.of(gcs.getString("serviceProject"), leonardo.getString("topicName"))

    private val retryConfig = GoogleTopicAdminInterpreter.defaultRetryConfig
    val publisherConfig: PublisherConfig = PublisherConfig(GCS.pathToQAJson, topic, retryConfig)
  }

  // for qaEmail and pathToQAPem and pathToQAJson
  object GCS extends CommonGCS {
    val pathToQAJson = gcs.getString("qaJsonFile")
  }

  // for NotebooksWhitelisted
  object Users extends CommonUsers
}
