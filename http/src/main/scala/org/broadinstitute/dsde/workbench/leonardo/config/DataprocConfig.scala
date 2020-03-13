package org.broadinstitute.dsde.workbench.leonardo.config

import org.broadinstitute.dsde.workbench.google2.{RegionName, ZoneName}
import org.broadinstitute.dsde.workbench.leonardo.CustomImage.DataprocCustomImage
import org.broadinstitute.dsde.workbench.leonardo.{MemorySize, RuntimeConfig}

final case class DataprocConfig(
  regionName: RegionName,
  zoneName: Option[ZoneName],
  defaultScopes: Set[String],
  legacyCustomDataprocImage: DataprocCustomImage,
  customDataprocImage: DataprocCustomImage,
  dataprocReservedMemory: Option[MemorySize],
  runtimeConfigDefaults: RuntimeConfig.DataprocConfig
)
