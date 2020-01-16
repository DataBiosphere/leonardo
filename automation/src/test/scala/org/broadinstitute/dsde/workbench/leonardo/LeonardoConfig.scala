package org.broadinstitute.dsde.workbench.leonardo

import com.google.pubsub.v1.ProjectTopicName
import org.broadinstitute.dsde.workbench.RetryConfig
import org.broadinstitute.dsde.workbench.config.CommonConfig
import org.broadinstitute.dsde.workbench.google2.{GoogleDataproc, PublisherConfig}
import org.broadinstitute.dsde.workbench.google2.GoogleTopicAdminInterpreter
import scala.concurrent.duration._

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
    val bioconductorImageUrl: String = leonardo.getString("bioconductorImageUrl")
    val rstudioBaseImageUrl: String = leonardo.getString("rstudioBaseImageUrl")

    private val topic = ProjectTopicName.of(leonardo.getString("pubsubGoogleProject"), leonardo.getString("topicName"))

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
