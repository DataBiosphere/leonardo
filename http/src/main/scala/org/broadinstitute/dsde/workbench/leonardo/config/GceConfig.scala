package org.broadinstitute.dsde.workbench.leonardo.config

import org.broadinstitute.dsde.workbench.google2.RegionName
import org.broadinstitute.dsde.workbench.leonardo.{MemorySize, RuntimeConfig}

case class GceConfig(regionName: RegionName,
                     defaultScopes: Set[String],
                     gceReservedMemory: Option[MemorySize],
                     runtimeConfigDefaults: RuntimeConfig.GceConfig)
