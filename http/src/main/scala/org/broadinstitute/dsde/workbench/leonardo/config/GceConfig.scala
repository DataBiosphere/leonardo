package org.broadinstitute.dsde.workbench.leonardo
package config

import org.broadinstitute.dsde.workbench.google2.DeviceName
import org.broadinstitute.dsde.workbench.leonardo.CustomImage.GceCustomImage

import scala.concurrent.duration.FiniteDuration

case class GceConfig(sourceImage: GceCustomImage,
                     userDiskDeviceName: DeviceName,
                     defaultScopes: Set[String],
                     gceReservedMemory: Option[MemorySizeBytes],
                     runtimeConfigDefaults: RuntimeConfig.GceConfig,
                     setMetadataPollDelay: FiniteDuration,
                     setMetadataPollMaxAttempts: Int
)
