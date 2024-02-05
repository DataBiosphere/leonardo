package org.broadinstitute.dsde.workbench.leonardo.config

final case class AzurePubSubConfig(
  topic: String,
  subscription: String,
  // required if using managed identity, but None if using connection string
  namespace: Option[String],
  // if not set, managed identity will be used
  connectionString: Option[String],
  // size of the in-memory queue the subscriber uses to buffer messages
  queueSize: Int
)
