package org.broadinstitute.dsde.workbench.leonardo.config

import org.broadinstitute.dsde.workbench.leonardo.{MemorySize, RuntimeConfig}

case class GceConfig(defaultScopes: Set[String],
                     gceReservedMemory: Option[MemorySize],
                     runtimeConfigDefaults: RuntimeConfig.GceConfig)
