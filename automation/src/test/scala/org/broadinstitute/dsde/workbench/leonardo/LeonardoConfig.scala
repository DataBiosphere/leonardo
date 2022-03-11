package org.broadinstitute.dsde.workbench.leonardo

import com.google.pubsub.v1.ProjectTopicName
import org.broadinstitute.dsde.workbench.config.CommonConfig
import org.broadinstitute.dsde.workbench.google2.{Location, PublisherConfig}

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
    val bioconductorImageUrl: String = leonardo.getString("bioconductorImageUrl")
    val rstudioBioconductorImage =
      ContainerImage(leonardo.getString("rstudioBioconductorImageUrl"), ContainerRegistry.GCR)

    private val topic = ProjectTopicName.of(gcs.getString("serviceProject"), leonardo.getString("topicName"))
    val location: Location = Location(leonardo.getString("location"))

    val publisherConfig: PublisherConfig = PublisherConfig(GCS.pathToQAJson, topic)

    val tenantId: String = leonardo.getString("tenantId")
    val subscriptionId: String = leonardo.getString("subscriptionId")
    val managedResourceGroup: String = leonardo.getString("managedResourceGroup")
  }

  // for qaEmail and pathToQAPem and pathToQAJson
  object GCS extends CommonGCS {
    val pathToQAJson = gcs.getString("qaJsonFile")
  }

  object WSM {
    val wsmUri: String = "https://workspace.dsde-dev.broadinstitute.org"
  }

  // for NotebooksWhitelisted
  object Users extends CommonUsers
}
