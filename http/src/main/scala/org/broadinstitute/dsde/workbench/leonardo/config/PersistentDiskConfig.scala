package org.broadinstitute.dsde.workbench.leonardo
package config
import org.broadinstitute.dsde.workbench.google2.ZoneName
import org.broadinstitute.dsde.workbench.leonardo.{BlockSize, DiskSize, DiskType}

final case class PersistentDiskConfig(
  defaultDiskSizeGB: DiskSize,
  defaultDiskType: DiskType,
  defaultBlockSizeBytes: BlockSize,
  defaultZone: ZoneName,
  defaultGalaxyNFSDiskSizeGB: DiskSize
)
