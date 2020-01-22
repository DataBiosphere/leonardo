package org.broadinstitute.dsde.workbench.leonardo.config

import org.broadinstitute.dsde.workbench.model.google.GoogleProject

final case class PubsubConfig(
  pubsubGoogleProject: GoogleProject,
  topicName: String,
  queueSize: Int
)
