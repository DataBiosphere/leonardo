package org.broadinstitute.dsde.workbench.leonardo
package config
import org.broadinstitute.dsde.workbench.google2.{DeviceName, RegionName, ZoneName}
import org.broadinstitute.dsde.workbench.leonardo.CustomImage.GceCustomImage
import org.broadinstitute.dsde.workbench.leonardo.{MemorySize, RuntimeConfig}

case class GceConfig(customGceImage: GceCustomImage,
                     userDiskDeviceName: DeviceName,
                     regionName: RegionName,
                     zoneName: ZoneName,
                     defaultScopes: Set[String],
                     gceReservedMemory: Option[MemorySize],
                     runtimeConfigDefaults: RuntimeConfig.GceConfig)
