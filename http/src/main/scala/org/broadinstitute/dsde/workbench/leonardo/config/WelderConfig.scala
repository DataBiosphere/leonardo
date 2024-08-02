package org.broadinstitute.dsde.workbench.leonardo
package config

final case class WelderConfig(
  deployWelderLabel: Option[String],
  updateWelderLabel: Option[String],
  deployWelderCutoffDate: Option[String],
  welderReservedMemory: Option[MemorySizeBytes]
)
