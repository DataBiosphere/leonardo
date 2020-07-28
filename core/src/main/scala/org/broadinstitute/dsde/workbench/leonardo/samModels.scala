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

sealed trait LeoAuthAction extends Product with Serializable {
  def asString: String
}
object LeoAuthAction {
  final case class Other(asString: String) extends LeoAuthAction

  val stringToAction: Map[String, LeoAuthAction] =
    sealerate.collect[LeoAuthAction].map(a => (a.asString, a)).toMap
}

sealed trait ProjectAction extends LeoAuthAction
object ProjectAction {
  final case object CreateRuntime extends ProjectAction {
    val asString = "launch_notebook_cluster"
  }
  final case object CreatePersistentDisk extends ProjectAction {
    val asString = "create_persistent_disk"
  }
  final case object CreateApp extends ProjectAction {
    val asString = "create_kubernetes_app"
  }

  // TODO we'd like to remove the below actions at the project level, and control these
  // actions with policies at the resource level instead. App resources currently follow
  // this model. See IA-XYZ.
  final case object GetRuntimeStatus extends ProjectAction {
    val asString = "list_notebook_cluster"
  }
  final case object DeleteRuntime extends ProjectAction {
    val asString = "delete_notebook_cluster"
  }
  final case object StopStartRuntime extends ProjectAction {
    val asString = "stop_start_notebook_cluster"
  }
  final case object ReadPersistentDisk extends ProjectAction {
    val asString = "list_persistent_disk"
  }
  final case object DeletePersistentDisk extends ProjectAction {
    val asString = "delete_persistent_disk"
  }

  val allActions = sealerate.values[ProjectAction]
}

sealed trait RuntimeAction extends LeoAuthAction
object RuntimeAction {
  final case object GetRuntimeStatus extends RuntimeAction {
    val asString = "status"
  }
  final case object ConnectToRuntime extends RuntimeAction {
    val asString = "connect"
  }
  final case object DeleteRuntime extends RuntimeAction {
    val asString = "delete"
  }
  final case object ModifyRuntime extends RuntimeAction {
    val asString = "modify"
  }
  final case object StopStartRuntime extends RuntimeAction {
    val asString = "stop_start"
  }
  val allActions = sealerate.values[RuntimeAction]
}

sealed trait PersistentDiskAction extends LeoAuthAction
object PersistentDiskAction {
  final case object ReadPersistentDisk extends PersistentDiskAction {
    val asString = "read"
  }
  final case object AttachPersistentDisk extends PersistentDiskAction {
    val asString = "attach"
  }
  final case object ModifyPersistentDisk extends PersistentDiskAction {
    val asString = "modify"
  }
  final case object DeletePersistentDisk extends PersistentDiskAction {
    val asString = "delete"
  }
  val allActions = sealerate.values[PersistentDiskAction]
}

sealed trait AppAction extends LeoAuthAction
object AppAction {
  final case object GetAppStatus extends AppAction {
    val asString = "status"
  }
  final case object ConnectToApp extends AppAction {
    val asString = "connect"
  }
  final case object UpdateApp extends AppAction {
    val asString = "update"
  }
  final case object DeleteApp extends AppAction {
    val asString = "delete"
  }
  val allActions = sealerate.values[AppAction]
}

sealed trait SamRole extends Product with Serializable {
  def asString: String
}
object SamRole {
  final case object Creator extends SamRole {
    val asString = "creator"
  }
  final case object Manager extends SamRole {
    val asString = "manager"
  }
  final case class Other(asString: String) extends SamRole
  val stringToRole = sealerate.collect[SamRole].map(p => (p.asString, p)).toMap
}

sealed trait SamPolicyName extends Serializable with Product
object SamPolicyName {
  final case object Creator extends SamPolicyName {
    override def toString = "creator"
  }
  final case object Owner extends SamPolicyName {
    override def toString = "owner"
  }
  final case object Manager extends SamPolicyName {
    override def toString = "manager"
  }
  final case class Other(asString: String) extends SamPolicyName {
    override def toString = asString
  }
  val stringToSamPolicyName: Map[String, SamPolicyName] =
    sealerate.collect[SamPolicyName].map(p => (p.toString, p)).toMap
}

final case class SamPolicyEmail(email: WorkbenchEmail) extends AnyVal
final case class SamPolicyData(actions: List[LeoAuthAction], memberEmails: List[WorkbenchEmail], roles: List[SamRole])
final case class SamResourcePolicy(samResource: SamResource, policyName: SamPolicyName)
