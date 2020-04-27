package org.broadinstitute.dsde.workbench.leonardo

import java.time.Instant

import enumeratum.{Enum, EnumEntry}
import org.broadinstitute.dsde.workbench.google2.{DiskName, ZoneName}
import org.broadinstitute.dsde.workbench.model.WorkbenchEmail
import org.broadinstitute.dsde.workbench.model.google.GoogleProject

final case class PersistentDisk(id: Long,
                                googleProject: GoogleProject,
                                zone: ZoneName,
                                name: DiskName,
                                googleId: Option[GoogleId],
                                samResourceId: DiskSamResourceId,
                                status: DiskStatus,
                                auditInfo: DiskAuditInfo,
                                size: DiskSize,
                                diskType: DiskType,
                                blockSize: BlockSize,
                                labels: LabelMap)

final case class DiskSamResourceId(asString: String) extends AnyVal

// See https://cloud.google.com/compute/docs/reference/rest/v1/disks
sealed trait DiskStatus extends EnumEntry
object DiskStatus extends Enum[DiskStatus] {
  val values = findValues

  final case object Creating extends DiskStatus
  final case object Restoring extends DiskStatus
  final case object Failed extends DiskStatus
  final case object Ready extends DiskStatus
  final case object Deleting extends DiskStatus
  final case object Deleted extends DiskStatus
}

final case class DiskAuditInfo(creator: WorkbenchEmail,
                               createdDate: Instant,
                               destroyedDate: Option[Instant],
                               dateAccessed: Instant)

// Disks are always specified in GB, it doesn't make sense to support other units
final case class DiskSize(gb: Int) extends AnyVal {
  def asString: String = s"$gb GB"
}

final case class BlockSize(bytes: Int) extends AnyVal

sealed trait DiskType extends EnumEntry {
  def googleString: String
}
object DiskType extends Enum[DiskType] {
  val values = findValues

  final case object Standard extends DiskType {
    override def googleString: String = "pd-standard"
  }
  final case object SSD extends DiskType {
    override def googleString: String = "pd-ssd"
  }
}
