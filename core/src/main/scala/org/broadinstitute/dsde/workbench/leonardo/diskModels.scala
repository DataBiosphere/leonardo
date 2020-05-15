package org.broadinstitute.dsde.workbench.leonardo

import ca.mrvisser.sealerate
import enumeratum.{Enum, EnumEntry}
import org.broadinstitute.dsde.workbench.google2.{DiskName, ZoneName}
import org.broadinstitute.dsde.workbench.model.WorkbenchEmail
import org.broadinstitute.dsde.workbench.model.google.GoogleProject

final case class PersistentDisk(id: DiskId,
                                googleProject: GoogleProject,
                                zone: ZoneName,
                                name: DiskName,
                                googleId: Option[GoogleId],
                                samResourceId: DiskSamResourceId,
                                status: DiskStatus,
                                auditInfo: AuditInfo,
                                size: DiskSize,
                                diskType: DiskType,
                                blockSize: BlockSize,
                                labels: LabelMap) {
  def projectNameString: String = s"${googleProject.value}/${name.value}"
}

final case class DiskId(value: Long) extends AnyVal
final case class DiskSamResourceId(asString: String) extends AnyVal

/** Default persistent disk labels */
case class DefaultDiskLabels(diskName: DiskName, googleProject: GoogleProject, creator: WorkbenchEmail) {
  def toMap: LabelMap =
    Map(
      "diskName" -> diskName.value,
      "googleProject" -> googleProject.value,
      "creator" -> creator.value
    ).filterNot(_._2 == null)
}

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

  //TODO: confirm these
  val activeStatuses: Set[DiskStatus] =
    Set(Creating, Restoring, Ready)

  val deletableStatuses: Set[DiskStatus] =
    Set(Failed, Ready)

  val updatableStatuses: Set[DiskStatus] = Set(Ready)

  implicit class EnrichedDiskStatus(status: DiskStatus) {
    def isActive: Boolean = activeStatuses contains status
    def isDeletable: Boolean = deletableStatuses contains status
    def isUpdatable: Boolean = updatableStatuses contains status
  }
}

// Disks are always specified in GB, it doesn't make sense to support other units
final case class DiskSize(gb: Int) extends AnyVal {
  def asString: String = s"$gb GB"
}

final case class BlockSize(bytes: Int) extends AnyVal

sealed trait DiskType extends Product with Serializable {
  def googleString: String
}
object DiskType {
  val allDiskTypes = sealerate.values[DiskType]

  final case object Standard extends DiskType {
    override def googleString: String = "pd-standard"
  }
  final case object SSD extends DiskType {
    override def googleString: String = "pd-ssd"
  }
  def stringToDiskType(string: String): Option[DiskType] =
    allDiskTypes
      .map(obj => obj.googleString -> obj)
      .toMap
      .get(string)
}
