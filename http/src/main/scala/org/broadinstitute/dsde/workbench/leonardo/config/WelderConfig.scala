package org.broadinstitute.dsde.workbench.leonardo.config

final case class WelderConfig(
  welderEnabledNotebooksDir: String,
  welderDisabledNotebooksDir: String, // TODO: remove once welder is rolled out to all clusters
  deployWelderLabel: Option[String],
  updateWelderLabel: Option[String],
  deployWelderCutoffDate: Option[String],
  welderReservedMemory: Option[MemoryConfig]
)
