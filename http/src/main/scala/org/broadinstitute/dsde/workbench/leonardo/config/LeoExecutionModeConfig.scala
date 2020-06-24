package org.broadinstitute.dsde.workbench.leonardo.config

sealed trait LeoExecutionModeConfig extends Product with Serializable
object LeoExecutionModeConfig {
  final case object BackLeoOnly extends LeoExecutionModeConfig
  final case object FrontLeoOnly extends LeoExecutionModeConfig
  final case object Combined extends LeoExecutionModeConfig
}
