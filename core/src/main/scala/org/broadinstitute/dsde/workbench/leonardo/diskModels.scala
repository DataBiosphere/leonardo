package org.broadinstitute.dsde.workbench.leonardo

import enumeratum.{Enum, EnumEntry}
import org.broadinstitute.dsde.workbench.google2.{DiskName, ZoneName}
import org.broadinstitute.dsde.workbench.leonardo.SamResourceId.PersistentDiskSamResourceId
import org.broadinstitute.dsde.workbench.model.WorkbenchEmail
import org.broadinstitute.dsde.workbench.model.google.GoogleProject

final case class PersistentDisk(id: DiskId,
                                cloudContext: CloudContext,
                                zone: ZoneName, // in the case of Azure, this will be com.azure.core.management.Region
                                name: DiskName,
                                serviceAccount: WorkbenchEmail,
                                samResource: PersistentDiskSamResourceId,
                                status: DiskStatus,
                                auditInfo: AuditInfo,
                                size: DiskSize,
                                diskType: DiskType,
                                blockSize: BlockSize,
                                formattedBy: Option[FormattedBy],
                                appRestore: Option[AppRestore],
                                labels: LabelMap,
                                sourceDisk: Option[DiskLink],
                                wsmResourceId: Option[WsmControlledResourceId],
                                workspaceId: Option[WorkspaceId]
) {
  def projectNameString: String = s"${cloudContext.asStringWithProvider}/${name.value}"
}

final case class DiskId(value: Long) extends AnyVal

/** Default persistent disk labels */
case class DefaultDiskLabels(diskName: DiskName,
                             cloudContext: CloudContext,
                             creator: WorkbenchEmail,
                             serviceAccount: WorkbenchEmail
) {
  def toMap: LabelMap =
    Map(
      "diskName" -> diskName.value,
      "googleProject" -> cloudContext.asString, // TODO: remove googleProject in the future.
      "cloudContext" -> cloudContext.asString,
      "creator" -> creator.value,
      "serviceAccount" -> serviceAccount.value
    ).filterNot(_._2 == null)
}

// See https://cloud.google.com/compute/docs/reference/rest/v1/disks
sealed trait DiskStatus extends EnumEntry
object DiskStatus extends Enum[DiskStatus] {
  val values = findValues

  // TODO: Create Pre statuses once https://github.com/DataBiosphere/leonardo/pull/1395/files#diff-4101c04c4a7015e058bf48267899df0bR92 is merged
  final case object Creating extends DiskStatus
  final case object Restoring extends DiskStatus
  final case object Failed extends DiskStatus
  final case object Ready extends DiskStatus
  final case object Deleting extends DiskStatus
  final case object Deleted extends DiskStatus
  final case object Error extends DiskStatus

  val activeStatuses: Set[DiskStatus] =
    Set(Creating, Restoring, Ready)

  val deletableStatuses: Set[DiskStatus] =
    Set(Failed, Ready, Error)

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

sealed trait DiskType extends EnumEntry with Product with Serializable {
  def asString: String
  def googleString(googleProject: GoogleProject, zoneName: ZoneName): String
}
object DiskType extends Enum[DiskType] {
  val values = findValues
  val stringToObject = values.map(v => v.asString -> v).toMap

  final case object Standard extends DiskType {
    override def asString: String = "pd-standard"
    def googleString(googleProject: GoogleProject, zoneName: ZoneName): String =
      s"projects/${googleProject.value}/zones/${zoneName.value}/diskTypes/pd-standard"
  }
  final case object SSD extends DiskType {
    override def asString: String = "pd-ssd"
    def googleString(googleProject: GoogleProject, zoneName: ZoneName): String =
      s"projects/${googleProject.value}/zones/${zoneName.value}/diskTypes/pd-ssd"
  }
  final case object Balanced extends DiskType {
    override def asString: String = "pd-balanced"
    def googleString(googleProject: GoogleProject, zoneName: ZoneName): String =
      s"projects/${googleProject.value}/zones/${zoneName.value}/diskTypes/pd-balanced"
  }
}

// We introduced this type to differentiate whether a disk is formatted by Galaxy or not because
// Galaxy formats disk in unique ways. We might be able to simplify this to be Galaxy vs non-Galaxy
sealed trait FormattedBy extends EnumEntry with Product with Serializable {
  def asString: String
}
object FormattedBy extends Enum[FormattedBy] {
  val values = findValues

  final case object GCE extends FormattedBy {
    override def asString: String = "GCE"
  }
  final case object Galaxy extends FormattedBy {
    override def asString: String = "GALAXY"
  }

  final case object Custom extends FormattedBy {
    override def asString: String = "CUSTOM"
  }

  final case object Cromwell extends FormattedBy {
    override def asString: String = "CROMWELL"
  }

  final case object Jupyter extends FormattedBy {
    override def asString: String = "JUPYTER"
  }

  final case object Allowed extends FormattedBy {
    override def asString: String = "ALLOWED"
  }
}

final case class PvcId(asString: String) extends AnyVal

sealed trait AppRestore extends Product with Serializable {
  def lastUsedBy: AppId
}

object AppRestore {
  // information needed for restoring a Galaxy app
  final case class GalaxyRestore(galaxyPvcId: PvcId, lastUsedBy: AppId) extends AppRestore

  // information needed for reconnecting a disk used previously by Cromwell app to another Cromwell app
  final case class Other(lastUsedBy: AppId) extends AppRestore
}

final case class DiskLink(asString: String) extends AnyVal

final case class SourceDisk(diskLink: DiskLink, formattedBy: Option[FormattedBy])
