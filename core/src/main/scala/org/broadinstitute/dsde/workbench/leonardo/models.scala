package org.broadinstitute.dsde.workbench.leonardo

import java.util.UUID
import ca.mrvisser.sealerate
import org.broadinstitute.dsde.workbench.azure.AzureCloudContext
import org.broadinstitute.dsde.workbench.model.google.GoogleProject

final case class WorkspaceId(value: UUID) extends AnyVal

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
