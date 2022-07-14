package org.broadinstitute.dsde.workbench.leonardo
package config

import org.broadinstitute.dsde.workbench.google2.ZoneName

final case class PersistentDiskConfig(
  defaultDiskSizeGb: DiskSize,
  defaultDiskType: DiskType,
  defaultBlockSizeBytes: BlockSize,
  defaultZone: ZoneName,
  defaultGalaxyNfsdiskSizeGb: DiskSize,
  dontCloneFromTheseGoogleFolders: Vector[String]
)
