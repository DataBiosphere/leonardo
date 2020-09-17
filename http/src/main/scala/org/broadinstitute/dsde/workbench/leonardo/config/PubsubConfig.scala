package org.broadinstitute.dsde.workbench.leonardo
package config
import org.broadinstitute.dsde.workbench.model.google.GoogleProject

final case class PubsubConfig(
  pubsubGoogleProject: GoogleProject,
  topicName: String,
  queueSize: Int
)
