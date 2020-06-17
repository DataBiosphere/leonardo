package org.broadinstitute.dsde.workbench.leonardo

import ca.mrvisser.sealerate
import org.broadinstitute.dsde.workbench.leonardo.SamResource.{PersistentDiskSamResource, RuntimeSamResource}
import org.broadinstitute.dsde.workbench.model.google.GoogleProject

sealed trait SamResource extends Product with Serializable {
  def resourceId: String
  def resourceType: SamResourceType
}
object SamResource {
  final case class RuntimeSamResource(resourceId: String) extends SamResource {
    val resourceType = SamResourceType.Runtime
  }
  final case class PersistentDiskSamResource(resourceId: String) extends SamResource {
    val resourceType = SamResourceType.PersistentDisk
  }
  final case class ProjectSamResource(googleProject: GoogleProject) extends SamResource {
    val resourceId = googleProject.value
    val resourceType = SamResourceType.Project
  }
  final case class AppSamResource(resourceId: String) extends SamResource {
    val resourceType = SamResourceType.App
  }
}

sealed trait SamResourceType extends Product with Serializable {
  def asString: String
}
object SamResourceType {
  final case object Project extends SamResourceType {
    val asString = "billing-project"
  }
  final case object Runtime extends SamResourceType {
    val asString = "notebook-cluster"
  }
  final case object PersistentDisk extends SamResourceType {
    val asString = "persistent-disk"
  }
  final case object App extends SamResourceType {
    val asString = "app"
  }
  val stringToSamResourceType: Map[String, SamResourceType] =
    sealerate.collect[SamResourceType].map(p => (p.asString, p)).toMap
}

sealed trait AccessPolicyName extends Serializable with Product
object AccessPolicyName {
  final case object Creator extends AccessPolicyName {
    override def toString = "creator"
  }
  final case object Owner extends AccessPolicyName {
    override def toString = "owner"
  }
  final case class Other(asString: String) extends AccessPolicyName {
    override def toString = asString
  }
  val stringToAccessPolicyName: Map[String, AccessPolicyName] =
    sealerate.collect[AccessPolicyName].map(p => (p.toString, p)).toMap
}

final case class SamRuntimePolicy(accessPolicyName: AccessPolicyName, resource: RuntimeSamResource)
final case class SamPersistentDiskPolicy(accessPolicyName: AccessPolicyName, resource: PersistentDiskSamResource)
final case class SamProjectPolicy(accessPolicyName: AccessPolicyName, googleProject: GoogleProject)
