package org.broadinstitute.dsde.workbench.leonardo
package config

import org.broadinstitute.dsde.workbench.google2.DeviceName

final case class SourceSnapShot(asString: String) extends AnyVal
case class GceConfig(sourceSnapshot: SourceSnapShot,
                     userDiskDeviceName: DeviceName,
                     defaultScopes: Set[String],
                     gceReservedMemory: Option[MemorySize],
                     runtimeConfigDefaults: RuntimeConfig.GceConfig)
