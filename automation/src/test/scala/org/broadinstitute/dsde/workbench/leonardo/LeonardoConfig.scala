package org.broadinstitute.dsde.workbench.leonardo

import com.google.pubsub.v1.ProjectTopicName
import org.broadinstitute.dsde.workbench.config.CommonConfig
import org.broadinstitute.dsde.workbench.google2.{Location, PublisherConfig}

object LeonardoConfig extends CommonConfig {
  private val leonardo = config.getConfig("leonardo")
  private val azure = config.getConfig("azure")
  private val gcs = config.getConfig("gcs")
  private val leonardoClient = config.getConfig("leonardoClient")

  object Leonardo {
    val apiUrl: String = leonardo.getString("apiUrl")
    val notebooksServiceAccountEmail: String = leonardo.getString("notebooksServiceAccountEmail")
    val rImageUrl: String = leonardo.getString("rImageUrl")
    val pythonImageUrl: String = leonardo.getString("pythonImageUrl")
    val hailImageUrl: String = leonardo.getString("hailImageUrl")
    val gatkImageUrl: String = leonardo.getString("gatkImageUrl")
    val aouImageUrl: String = leonardo.getString("aouImageUrl")
    val baseImageUrl: String = leonardo.getString("baseImageUrl")
    val rstudioBioconductorImage =
      ContainerImage(leonardo.getString("rstudioBioconductorImageUrl"), ContainerRegistry.GCR)

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

  object Azure {
    val vmUser = azure.getString("leoVmUser")
    val vmPassword = azure.getString("leoVmPassword")
    val bastionName = azure.getString("bastionName")
    val defaultBastionPort = azure.getInt("defaultBastionPort")
  }

  // TODO: this should be updated once we're able to run azure automation tests as part of CI
  object WSM {
    val wsmUri: String = "https://workspace.dsde-dev.broadinstitute.org"
  }

  object BPM {
    val bpmUri: String = "https://bpm.dsde-dev.broadinstitute.org"
  }

  object LeonardoClient {
    val writeTimeout = leonardoClient.getInt("writeTimeout")
    val readTimeout = leonardoClient.getInt("readTimeout")
    val connectionTimeout = leonardoClient.getInt("connectionTimeout")
  }

  // for NotebooksWhitelisted
  object Users extends CommonUsers
}
