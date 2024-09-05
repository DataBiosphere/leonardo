package org.broadinstitute.dsde.workbench.leonardo
package http

import org.broadinstitute.dsde.workbench.google2.{DiskName, ZoneName}
import org.broadinstitute.dsde.workbench.leonardo.SamResourceId.PersistentDiskSamResourceId
import org.broadinstitute.dsde.workbench.model.WorkbenchEmail
import org.broadinstitute.dsde.workbench.model.google.GoogleProject

final case class CreateDiskRequest(labels: LabelMap,
                                   size: Option[DiskSize],
                                   diskType: Option[DiskType],
                                   blockSize: Option[BlockSize],
                                   zone: Option[ZoneName],
                                   sourceDisk: Option[SourceDiskRequest]
)

final case class SourceDiskRequest(googleProject: GoogleProject, name: DiskName)

object CreateDiskRequest {
  def fromDiskConfigRequest(create: PersistentDiskRequest, zone: Option[ZoneName]): CreateDiskRequest =
    CreateDiskRequest(create.labels, create.size, create.diskType, None, zone, None)
}

final case class PersistentDiskRequest(name: DiskName,
                                       size: Option[DiskSize],
                                       diskType: Option[DiskType],
                                       labels: LabelMap
)

final case class DiskConfig(name: DiskName, size: DiskSize, diskType: DiskType, blockSize: BlockSize)
object DiskConfig {
  def fromPersistentDisk(disk: PersistentDisk): DiskConfig =
    DiskConfig(disk.name, disk.size, disk.diskType, disk.blockSize)
}

final case class ListPersistentDiskResponse(id: DiskId,
                                            cloudContext: CloudContext,
                                            zone: ZoneName,
                                            name: DiskName,
                                            status: DiskStatus,
                                            auditInfo: AuditInfo,
                                            size: DiskSize,
                                            diskType: DiskType,
                                            blockSize: BlockSize,
                                            labels: LabelMap,
                                            workspaceId: Option[WorkspaceId]
)

final case class GetPersistentDiskResponse(id: DiskId,
                                           cloudContext: CloudContext,
                                           zone: ZoneName,
                                           name: DiskName,
                                           serviceAccount: WorkbenchEmail,
                                           samResource: PersistentDiskSamResourceId,
                                           status: DiskStatus,
                                           auditInfo: AuditInfo,
                                           size: DiskSize,
                                           diskType: DiskType,
                                           blockSize: BlockSize,
                                           labels: LabelMap,
                                           formattedBy: Option[FormattedBy],
                                           workspaceId: Option[WorkspaceId]
)

final case class GetPersistentDiskV2Response(id: DiskId,
                                             cloudContext: CloudContext,
                                             zone: ZoneName,
                                             name: DiskName,
                                             serviceAccount: WorkbenchEmail,
                                             samResource: PersistentDiskSamResourceId,
                                             status: DiskStatus,
                                             auditInfo: AuditInfo,
                                             size: DiskSize,
                                             diskType: DiskType,
                                             blockSize: BlockSize,
                                             labels: LabelMap,
                                             workspaceId: Option[WorkspaceId],
                                             formattedBy: Option[FormattedBy]
)

final case class UpdateDiskRequest(labels: LabelMap, size: DiskSize)
