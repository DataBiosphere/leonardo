package org.broadinstitute.dsde.workbench.leonardo.config

import org.broadinstitute.dsde.workbench.google2.{RegionName, ZoneName}
import org.broadinstitute.dsde.workbench.leonardo.CustomImage.GceCustomImage
import org.broadinstitute.dsde.workbench.leonardo.{DiskSize, MemorySize, RuntimeConfig}

case class GceConfig(bootDiskSize: DiskSize,
                     customGceImage: GceCustomImage,
                     regionName: RegionName,
                     zoneName: ZoneName,
                     defaultScopes: Set[String],
                     gceReservedMemory: Option[MemorySize],
                     runtimeConfigDefaults: RuntimeConfig.GceConfig)
