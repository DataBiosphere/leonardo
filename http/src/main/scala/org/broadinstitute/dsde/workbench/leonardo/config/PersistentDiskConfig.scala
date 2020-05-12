package org.broadinstitute.dsde.workbench.leonardo.config

import org.broadinstitute.dsde.workbench.leonardo.{BlockSize, DiskSize, DiskType}

case class PersistentDiskConfig(
  defaultDiskSizeGB: DiskSize,
  defaultDiskType: DiskType,
  defaultBlockSizeBytes: BlockSize
)
