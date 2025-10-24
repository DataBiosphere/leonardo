package org.broadinstitute.dsde.workbench.leonardo

import ca.mrvisser.sealerate
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
}

sealed abstract class CloudProvider extends Product with Serializable {
  def asString: String
}
object CloudProvider {
  final case object Gcp extends CloudProvider {
    override val asString = "GCP"
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
}
