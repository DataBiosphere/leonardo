package org.broadinstitute.dsde.workbench.leonardo.config

import org.broadinstitute.dsde.workbench.leonardo.MemorySize

// TODO add default machine config
case class GceConfig(defaultScopes: Set[String], gceReservedMemory: Option[MemorySize])
