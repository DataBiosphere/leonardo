package org.broadinstitute.dsde.workbench.leonardo.config

import org.broadinstitute.dsde.workbench.leonardo.{CustomDataprocImage, MemorySize}

final case class DataprocConfig(
  dataprocDefaultRegion: String,
  dataprocZone: Option[String],
  defaultScopes: Set[String],
  legacyCustomDataprocImage: CustomDataprocImage,
  customDataprocImage: CustomDataprocImage,
  dataprocReservedMemory: Option[MemorySize]
)
