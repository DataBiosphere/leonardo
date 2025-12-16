package org.broadinstitute.dsde.workbench.leonardo

import com.google.pubsub.v1.ProjectTopicName
import org.broadinstitute.dsde.workbench.config.CommonConfig
import org.broadinstitute.dsde.workbench.google2.{Location, PublisherConfig}

object LeonardoConfig extends CommonConfig {
  private val leonardo = config.getConfig("leonardo")
  private val gcs = config.getConfig("gcs")
  private val leonardoClient = config.getConfig("leonardoClient")

  object Leonardo {
    val apiUrl: String = leonardo.getString("apiUrl")
    val notebooksServiceAccountEmail: String = leonardo.getString("notebooksServiceAccountEmail")
    val baseImageUrl: String = leonardo.getString("baseImageUrl")
    val rImageUrl: String = leonardo.getString("rImageUrl")
    val pythonImageUrl: String = leonardo.getString("pythonImageUrl")
    val hailImageUrl: String = leonardo.getString("hailImageUrl")
    val gatkImageUrl: String = leonardo.getString("gatkImageUrl")
    val aouImageUrl: String = leonardo.getString("aouImageUrl")
    val rstudioBioconductorImage =
      ContainerImage(leonardo.getString("rstudioBioconductorImageUrl"), ContainerRegistry.GAR)

    private val topic = ProjectTopicName.of(gcs.getString("serviceProject"), leonardo.getString("topicName"))
    val location: Location = Location(leonardo.getString("location"))

    val publisherConfig: PublisherConfig = PublisherConfig(GCS.pathToQAJson, topic)

    val serviceAccountEmail = leonardo.getString("serviceAccountEmail")
  }

  // for qaEmail and pathToQAPem and pathToQAJson
  object GCS extends CommonGCS {
    val pathToQAJson = gcs.getString("qaJsonFile")
    val leonardoServiceAccountUsername = gcs.getString("leonardoServiceAccountUsername")
  }

  object LeonardoClient {
    val writeTimeout = leonardoClient.getInt("writeTimeout")
    val readTimeout = leonardoClient.getInt("readTimeout")
    val connectionTimeout = leonardoClient.getInt("connectionTimeout")
  }

  // for NotebooksWhitelisted
  object Users extends CommonUsers
}
