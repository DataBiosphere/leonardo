package org.broadinstitute.dsde.workbench.leonardo

import ca.mrvisser.sealerate
import org.broadinstitute.dsde.workbench.model.WorkbenchEmail
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
  final case object Manager extends AccessPolicyName {
    override def toString = "manager"
  }
  final case class Other(asString: String) extends AccessPolicyName {
    override def toString = asString
  }
  val stringToAccessPolicyName: Map[String, AccessPolicyName] =
    sealerate.collect[AccessPolicyName].map(p => (p.toString, p)).toMap
}

final case class SamResourcePolicy(samResource: SamResource, policyName: AccessPolicyName)
final case class SamPolicyEmail(email: WorkbenchEmail) extends AnyVal

sealed trait LeoAuthAction extends Product with Serializable {
  def asString: String
}

sealed trait ProjectAction extends LeoAuthAction
object ProjectAction {
  case object CreateRuntime extends ProjectAction {
    val asString = "launch_notebook_cluster"
  }
  case object CreatePersistentDisk extends ProjectAction {
    val asString = "create_persistent_disk"
  }
  case object CreateApp extends ProjectAction {
    val asString = "create_kubernetes_app"
  }

  // TODO we'd like to remove these actions at the project level, and control this with
  // policies at the resource level instead. App resources currently follow this model.
  case object GetRuntimeStatus extends ProjectAction {
    val asString = "list_notebook_cluster"
  }
  case object DeleteRuntime extends ProjectAction {
    val asString = "delete_notebook_cluster"
  }
  case object StopStartRuntime extends ProjectAction {
    val asString = "stop_start_notebook_cluster"
  }
  case object ReadPersistentDisk extends ProjectAction {
    val asString = "list_persistent_disk"
  }
  case object DeletePersistentDisk extends ProjectAction {
    val asString = "delete_persistent_disk"
  }

  val allActions = sealerate.values[ProjectAction]
}

sealed trait RuntimeAction extends LeoAuthAction
object RuntimeAction {
  case object GetRuntimeStatus extends RuntimeAction {
    val asString = "status"
  }
  case object ConnectToRuntime extends RuntimeAction {
    val asString = "connect"
  }
  case object DeleteRuntime extends RuntimeAction {
    val asString = "delete"
  }
  case object ModifyRuntime extends RuntimeAction {
    val asString = "modify"
  }
  case object StopStartRuntime extends RuntimeAction {
    val asString = "stop_start"
  }
  val allActions = sealerate.values[RuntimeAction]
}

sealed trait PersistentDiskAction extends LeoAuthAction
object PersistentDiskAction {
  case object ReadPersistentDisk extends PersistentDiskAction {
    val asString = "read"
  }
  case object AttachPersistentDisk extends PersistentDiskAction {
    val asString = "attach"
  }
  case object ModifyPersistentDisk extends PersistentDiskAction {
    val asString = "modify"
  }
  case object DeletePersistentDisk extends PersistentDiskAction {
    val asString = "delete"
  }
  val allActions = sealerate.values[PersistentDiskAction]
}

sealed trait AppAction extends LeoAuthAction
object AppAction {
  case object GetAppStatus extends AppAction {
    val asString = "status"
  }
  case object ConnectToApp extends AppAction {
    val asString = "connect"
  }
  case object UpdateApp extends AppAction {
    val asString = "update"
  }
  case object DeleteApp extends AppAction {
    val asString = "delete"
  }
  val allActions = sealerate.values[AppAction]
}
