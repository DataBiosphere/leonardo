package org.broadinstitute.dsde.workbench.leonardo

import bio.terra.workspace.model.State
import ca.mrvisser.sealerate
import cats.effect.IO
import org.broadinstitute.dsde.workbench.azure.AzureCloudContext
import org.broadinstitute.dsde.workbench.model.google.{GcsBucketName, GoogleProject}
import org.typelevel.log4cats.StructuredLogger

import java.util.UUID

final case class WorkspaceId(value: UUID) extends AnyVal

final case class BillingProfileId(value: String) extends AnyVal

final case class CloudContextDb(value: String) extends AnyVal

sealed abstract class CloudContext extends Product with Serializable {
  def asString: String
  def asStringWithProvider: String
  def cloudProvider: CloudProvider
  def asCloudContextDb: CloudContextDb = CloudContextDb(asString)
}
object CloudContext {
  final case class Gcp(value: GoogleProject) extends CloudContext {
    override val asString = value.value
    override val asStringWithProvider = s"Gcp/${value.value}"
    override def cloudProvider: CloudProvider = CloudProvider.Gcp
  }
  final case class Azure(value: AzureCloudContext) extends CloudContext {
    override val asString = value.asString
    override val asStringWithProvider = s"Azure/${value.asString}"
    override def cloudProvider: CloudProvider = CloudProvider.Azure
  }
}

sealed abstract class CloudProvider extends Product with Serializable {
  def asString: String
}
object CloudProvider {
  final case object Gcp extends CloudProvider {
    override val asString = "GCP"
  }
  final case object Azure extends CloudProvider {
    override val asString = "AZURE"
  }

  val stringToCloudProvider = sealerate.values[CloudProvider].map(p => (p.asString, p)).toMap
}

sealed abstract class StagingBucket extends Product with Serializable {
  def asString: String
}
object StagingBucket {
  final case class Gcp(value: GcsBucketName) extends StagingBucket {
    override def asString: String = value.value
  }
  final case class Azure(storageContainerName: org.broadinstitute.dsde.workbench.azure.ContainerName)
      extends StagingBucket {
    override def asString: String = s"${storageContainerName.value}"
  }
}

/**
 * Can't extend final enum State from WSM, so made a wrapper
 * WSM state can be BROKEN, CREATING, DELETING, READY, UPDATING or None
 * if None --> it is deletable and is deleted
 * (already deleted in WSM, need to clean up leo resources)
 */
case class WsmState(state: Option[String])(implicit log: StructuredLogger[IO]) {

  val deletableStatuses: Set[String] = Set("BROKEN", "READY", "DELETED")
  val possibleStatuses: Array[String] = State.values().map(_.toString) :+ "DELETED"

  def apply(): Unit =
    if (!possibleStatuses.contains(this.value)) {
      log.warn(s"Unrecognized WSM state $state, WSM resource may not be processed correctly")
    }
  def value: String = state.getOrElse("DELETED").toUpperCase()

  /** Any in-progress state cannot be deleted: CREATING, DELETING, UPDATING */
  def isDeletable: Boolean = deletableStatuses contains this.value

  def isDeleted: Boolean = this.value == "DELETED"
}

//sealed trait WsmStatus {
//  val value: String
//}
//
//object WsmStatus {
//  case object BROKEN extends WsmStatus {
//    val value = "BROKEN"
//  }
//
//  case object READY extends WsmStatus {
//    val value = "READY"
//  }
//
//  case object DELETED extends WsmStatus {
//    val value = "DELETED"
//  }
//
//  case object CREATING extends WsmStatus {
//    val value = "CREATING"
//  }
//
//  case object DELETING extends WsmStatus {
//    val value = "DELETING"
//  }
//
//  case object UPDATING extends WsmStatus {
//    val value = "UPDATING"
//  }
//
//  case object UNKNOWN extends WsmStatus {
//    val value = "UNKNOWN"
//  }
//
//  // Function to create a WsmStatus instance from a string
//  def apply(value: Option[String]): WsmStatus =
//    value match {
//      case "BROKEN"   => BROKEN
//      case "READY"    => READY
//      case "CREATING" => CREATING
//      case "DELETING" => DELETING
//      case "UPDATING" => UPDATING
//      case None       => DELETED
//      case _          => UNKNOWN
//    }
//
//  val deletableStatuses: Set[WsmStatus] = Set(READY, BROKEN, DELETED)
//
//  implicit class EnrichedWsmStatus(status: WsmStatus) {
//    def isDeletable: Boolean = deletableStatuses contains status
//    def isDeleted: Boolean = status == DELETED
//
//  }
//}
