package org.broadinstitute.dsde.workbench.leonardo

import ca.mrvisser.sealerate
import org.broadinstitute.dsde.workbench.azure.AzureCloudContext
import org.broadinstitute.dsde.workbench.model.google.{GcsBucketName, GoogleProject}

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
case class WsmState(value: Option[String]) {
  def getValue: String = value.getOrElse("DELETED")

  /** Any in-progress state cannot be deleted:
CREATING, DELETING, UPDATING
 */
  def isDeletable: Boolean = value match {
    case Some(s) => Set("BROKEN", "READY") contains s
    case _       => true
  }
  def isDeleted: Boolean = value match {
    case Some(_) => false
    case _       => true
  }
}
