package org.broadinstitute.dsde.workbench.leonardo.config

import java.nio.file.Path

import org.broadinstitute.dsde.workbench.leonardo.MemorySize

final case class WelderConfig(
  welderEnabledNotebooksDir: Path,
  welderDisabledNotebooksDir: Path, // TODO: remove once welder is rolled out to all clusters
  deployWelderLabel: Option[String],
  updateWelderLabel: Option[String],
  deployWelderCutoffDate: Option[String],
  welderReservedMemory: Option[MemorySize]
)
