package org.broadinstitute.dsde.workbench.leonardo

import ca.mrvisser.sealerate
import org.broadinstitute.dsde.workbench.leonardo.SamResourceId.RuntimeSamResourceId
import org.broadinstitute.dsde.workbench.model.WorkbenchEmail
import org.broadinstitute.dsde.workbench.model.google.GoogleProject

/** A Sam resource type and resource ID */
sealed trait SamResourceId {
  def resourceId: String
  def resourceType: SamResourceType
  def asString: String = resourceId
}
object SamResourceId {
  final case class RuntimeSamResourceId(resourceId: String) extends SamResourceId {
    override def resourceType: SamResourceType = SamResourceType.Runtime
  }

  final case class PersistentDiskSamResourceId(resourceId: String) extends SamResourceId {
    override def resourceType: SamResourceType = SamResourceType.PersistentDisk
  }

  final case class ProjectSamResourceId(googleProject: GoogleProject) extends SamResourceId {
    override def resourceId: String = googleProject.value
    override def resourceType: SamResourceType = SamResourceType.Project
  }

  final case class AppSamResourceId(resourceId: String, accessScope: Option[AppAccessScope]) extends SamResourceId {
    override def resourceType: SamResourceType = accessScope match {
      case Some(AppAccessScope.UserPrivate)     => SamResourceType.App
      case Some(AppAccessScope.WorkspaceShared) => SamResourceType.SharedApp
      case None                                 => SamResourceType.App
    }
  }

  final case class WorkspaceResourceSamResourceId(workspaceId: WorkspaceId) extends SamResourceId {
    override def resourceId: String = workspaceId.value.toString
    override def resourceType: SamResourceType = SamResourceType.Workspace
  }

  final case class WsmResourceSamResourceId(controlledResourceId: WsmControlledResourceId) extends SamResourceId {
    override def resourceId: String = controlledResourceId.value.toString
    override def resourceType: SamResourceType = SamResourceType.WsmResource
  }

  final case class PrivateAzureStorageAccountSamResourceId(resourceId: String) extends SamResourceId {
    override def resourceType = SamResourceType.PrivateAzureStorageAccount
  }
}

/** Enumeration of Sam resource types known to Leonardo */
sealed trait SamResourceType extends Product with Serializable {
  def asString: String
}
object SamResourceType {
  final case object Project extends SamResourceType {
    val asString = "google-project"
  }
  final case object Runtime extends SamResourceType {
    val asString = "notebook-cluster"
  }
  final case object PersistentDisk extends SamResourceType {
    val asString = "persistent-disk"
  }
  final case object App extends SamResourceType {
    val asString = "kubernetes-app"
  }
  final case object SharedApp extends SamResourceType {
    val asString = "kubernetes-app-shared"
  }
  final case object Workspace extends SamResourceType {
    val asString = "workspace"
  }
  final case object WsmResource extends SamResourceType {
    val asString = "controlled-application-private-workspace-resource"
  }
  final case object PrivateAzureStorageAccount extends SamResourceType {
    val asString = "private_azure_storage_account"
  }

  val stringToSamResourceType: Map[String, SamResourceType] =
    sealerate.collect[SamResourceType].map(p => (p.asString, p)).toMap
}

/** Enumeration of Sam resource actions known to Leonardo */
sealed trait SamResourceAction extends Product with Serializable {
  def asString: String
  override def toString = asString
}

sealed trait ProjectAction extends SamResourceAction
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

  // TODO the below actions will be removed after we migrate to Sam hierarchical resources
  //  for notebook-cluster and persistent-disk. See: https://broadworkbench.atlassian.net/browse/IA-5059

  // Other exists because there are other project actions not used by Leo
  final case class Other(asString: String) extends ProjectAction

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
  val allActions = sealerate.collect[ProjectAction]
  val stringToAction: Map[String, ProjectAction] =
    sealerate.collect[ProjectAction].map(a => (a.asString, a)).toMap
}

sealed trait RuntimeAction extends SamResourceAction
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
  final case object ReadPolicies extends RuntimeAction {
    val asString = "read_policies"
  }
  final case object SetParent extends RuntimeAction {
    val asString = "set_parent"
  }

  val allActions = sealerate.values[RuntimeAction]
  val stringToAction: Map[String, RuntimeAction] =
    sealerate.collect[RuntimeAction].map(a => (a.asString, a)).toMap
}

sealed trait PersistentDiskAction extends SamResourceAction
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
  final case object ReadPolicies extends PersistentDiskAction {
    val asString = "read_policies"
  }
  final case object SetParent extends PersistentDiskAction {
    val asString = "set_parent"
  }

  val allActions = sealerate.values[PersistentDiskAction]
  val stringToAction: Map[String, PersistentDiskAction] =
    sealerate.collect[PersistentDiskAction].map(a => (a.asString, a)).toMap
}

sealed trait AppAction extends SamResourceAction
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
  final case object StopApp extends AppAction {
    val asString = "stop"
  }
  final case object StartApp extends AppAction {
    val asString = "start"
  }
  final case object ReadPolicies extends AppAction {
    val asString = "read_policies"
  }
  final case object SetParent extends AppAction {
    val asString = "set_parent"
  }

  val allActions = sealerate.values[AppAction]
  val stringToAction: Map[String, AppAction] =
    sealerate.collect[AppAction].map(a => (a.asString, a)).toMap
}

sealed trait WorkspaceAction extends SamResourceAction
object WorkspaceAction {
  final case object CreateControlledApplicationResource extends WorkspaceAction {
    val asString = "create_controlled_application_private"
  }
  final case object CreateControlledUserResource extends WorkspaceAction {
    val asString = "create_controlled_user_private"
  }
  final case object Delete extends WorkspaceAction {
    val asString = "delete"
  }

  val allActions = sealerate.values[WorkspaceAction]
  val stringToAction: Map[String, WorkspaceAction] =
    sealerate.collect[WorkspaceAction].map(a => (a.asString, a)).toMap
}

sealed trait WsmResourceAction extends SamResourceAction
object WsmResourceAction {
  final case object Write extends WsmResourceAction {
    val asString = "write"
  }
  final case object Read extends WsmResourceAction {
    val asString = "read"
  }
  val allActions = sealerate.values[WsmResourceAction]
  val stringToAction: Map[String, WsmResourceAction] =
    sealerate.collect[WsmResourceAction].map(a => (a.asString, a)).toMap
}

sealed trait PrivateAzureStorageAccountAction extends SamResourceAction
object PrivateAzureStorageAccountAction {
  final case object Write extends PrivateAzureStorageAccountAction {
    val asString = "write"
  }
  final case object Read extends PrivateAzureStorageAccountAction {
    val asString = "read"
  }
  val allActions = sealerate.values[PrivateAzureStorageAccountAction]
  val stringToAction: Map[String, PrivateAzureStorageAccountAction] =
    sealerate.collect[PrivateAzureStorageAccountAction].map(a => (a.asString, a)).toMap
}

/** Enumeration of Sam resource roles known to Leonardo. */
sealed trait SamResourceRole extends Product with Serializable {
  def asString: String
  override def toString = asString
}

sealed trait RuntimeRole extends SamResourceRole
object RuntimeRole {
  final case object Creator extends RuntimeRole {
    val asString = "creator"
  }

  final case object Manager extends RuntimeRole {
    val asString = "manager"
  }
}

sealed trait PersistentDiskRole extends SamResourceRole
object PersistentDiskRole {
  final case object Creator extends PersistentDiskRole {
    val asString = "creator"
  }

  final case object Manager extends PersistentDiskRole {
    val asString = "manager"
  }
}

sealed trait AppRole extends SamResourceRole
object AppRole {
  final case object Creator extends AppRole {
    val asString = "creator"
  }

  final case object Manager extends AppRole {
    val asString = "manager"
  }
}

sealed trait SharedAppRole extends SamResourceRole
object SharedAppRole {
  final case object Owner extends SharedAppRole {
    val asString = "owner"
  }

  final case object User extends SharedAppRole {
    val asString = "user"
  }
}

/**
 * Deprecated: use resource-type specific role enums (RuntimeRole, PersistentDiskRole, etc).
 */
@Deprecated
sealed trait SamRole extends Product with Serializable {
  def asString: String
  override def toString = asString
}
object SamRole {

  final case object Creator extends SamRole {
    val asString = "creator"
  }
  final case object Manager extends SamRole {
    val asString = "manager"
  }
  final case object Writer extends SamRole {
    val asString = "writer"
  }
  final case object Owner extends SamRole {
    val asString = "owner"
  }
  final case object User extends SamRole {
    val asString = "user"
  }
  final case class Other(asString: String) extends SamRole
  val stringToRole = sealerate.collect[SamRole].map(p => (p.asString, p)).toMap
}

/**
 * Deprecated: don't use an enum to represent policy names.
 */
@Deprecated
sealed trait SamPolicyName extends Serializable with Product
object SamPolicyName {
  final case object Creator extends SamPolicyName {
    override def toString = "creator"
  }
  final case object Writer extends SamPolicyName {
    override def toString = "writer"
  }
  final case object Owner extends SamPolicyName {
    override def toString = "owner"
  }
  final case object Manager extends SamPolicyName {
    override def toString = "manager"
  }
  final case object User extends SamPolicyName {
    override def toString = "user"
  }
  final case class Other(asString: String) extends SamPolicyName {
    override def toString = asString
  }
  val stringToSamPolicyName: Map[String, SamPolicyName] =
    sealerate.collect[SamPolicyName].map(p => (p.toString, p)).toMap
}

final case class SamPolicyEmail(email: WorkbenchEmail) extends AnyVal
final case class SamPolicyData(memberEmails: List[WorkbenchEmail], roles: List[String])

sealed abstract class AppAccessScope
object AppAccessScope {
  case object UserPrivate extends AppAccessScope {
    override def toString: String = "USER_PRIVATE"
  }
  case object WorkspaceShared extends AppAccessScope {
    override def toString: String = "WORKSPACE_SHARED"
  }

  def values: Set[AppAccessScope] = sealerate.values[AppAccessScope]

  def stringToObject: Map[String, AppAccessScope] = values.map(v => v.toString -> v).toMap
}

/** The response format for id-only runtime database calls. */
final case class ListRuntimeIdResponse(id: Long, samResource: RuntimeSamResourceId)
