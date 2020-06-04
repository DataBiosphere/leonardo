package org.broadinstitute.dsde.workbench.leonardo
package http

import org.broadinstitute.dsde.workbench.google2.DiskName

final case class CreateDiskRequest(labels: LabelMap,
                                   size: Option[DiskSize],
                                   diskType: Option[DiskType],
                                   blockSize: Option[BlockSize])

object CreateDiskRequest {
  def fromDiskConfigRequest(create: PersistentDiskRequest): CreateDiskRequest =
    CreateDiskRequest(create.labels, create.size, create.diskType, create.blockSize)
}

final case class PersistentDiskRequest(name: DiskName,
                                       size: Option[DiskSize],
                                       diskType: Option[DiskType],
                                       blockSize: Option[BlockSize],
                                       labels: LabelMap)

final case class DiskConfig(name: DiskName, size: DiskSize, diskType: DiskType, blockSize: BlockSize)
object DiskConfig {
  def fromPersistentDisk(disk: PersistentDisk): DiskConfig =
    DiskConfig(disk.name, disk.size, disk.diskType, disk.blockSize)
}
