package org.broadinstitute.dsde.workbench.leonardo.config

import org.broadinstitute.dsde.workbench.google2.ZoneName
import org.broadinstitute.dsde.workbench.leonardo.{GceCustomImage, MemorySize, RuntimeConfig}

case class GceConfig(customGceImage: GceCustomImage,
                     zoneName: ZoneName,
                     defaultScopes: Set[String],
                     gceReservedMemory: Option[MemorySize],
                     runtimeConfigDefaults: RuntimeConfig.GceConfig)
