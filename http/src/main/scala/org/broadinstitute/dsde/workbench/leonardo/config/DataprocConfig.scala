package org.broadinstitute.dsde.workbench.leonardo.config

import org.broadinstitute.dsde.workbench.google2.{RegionName, ZoneName}
import org.broadinstitute.dsde.workbench.leonardo.{CustomDataprocImage, MemorySize, RuntimeConfig}

final case class DataprocConfig(
  regionName: RegionName,
  zoneName: Option[ZoneName],
  defaultScopes: Set[String],
  legacyCustomDataprocImage: CustomDataprocImage,
  customDataprocImage: CustomDataprocImage,
  dataprocReservedMemory: Option[MemorySize],
  runtimeConfigDefaults: RuntimeConfig.DataprocConfig
)
