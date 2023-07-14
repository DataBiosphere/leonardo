package org.broadinstitute.dsde.workbench.leonardo
package model

import cats.data.NonEmptyList
import cats.mtl.Ask
import io.circe.{Decoder, Encoder}
import org.broadinstitute.dsde.workbench.leonardo.SamResourceId._
import org.broadinstitute.dsde.workbench.leonardo.model.SamResource.{
  AppSamResource,
  PersistentDiskSamResource,
  ProjectSamResource,
  RuntimeSamResource,
  WorkspaceResource,
  WsmResource
}
import org.broadinstitute.dsde.workbench.leonardo.dao.HttpSamDAO._
import org.broadinstitute.dsde.workbench.model.google.GoogleProject
import org.broadinstitute.dsde.workbench.model.{TraceId, UserInfo, WorkbenchEmail}

// Typeclass representing a Sam resource and associated policies
sealed trait SamResource[R] {
  def resourceType(r: R): SamResourceType
  def resourceIdAsString(r: R): String
  def policyNames(r: R): Set[SamPolicyName]
  def ownerRoleName(r: R): SamRole
}
object SamResource {
  class ProjectSamResource extends SamResource[ProjectSamResourceId] {
    def resourceType(r: ProjectSamResourceId) = SamResourceType.Project
    def policyNames(r: ProjectSamResourceId) = Set(SamPolicyName.Owner)
    def ownerRoleName(r: ProjectSamResourceId) = SamRole.Owner
    def resourceIdAsString(r: ProjectSamResourceId): String = r.googleProject.value
  }
  class RuntimeSamResource extends SamResource[RuntimeSamResourceId] {
    def resourceType(r: RuntimeSamResourceId) = SamResourceType.Runtime
    def policyNames(r: RuntimeSamResourceId) = Set(SamPolicyName.Creator)
    def ownerRoleName(r: RuntimeSamResourceId) = SamRole.Creator
    def resourceIdAsString(r: RuntimeSamResourceId): String = r.resourceId
  }
  class PersistentDiskSamResource extends SamResource[PersistentDiskSamResourceId] {
    def resourceType(r: PersistentDiskSamResourceId) = SamResourceType.PersistentDisk
    def policyNames(r: PersistentDiskSamResourceId) = Set(SamPolicyName.Creator)
    def ownerRoleName(r: PersistentDiskSamResourceId) = SamRole.Creator
    def resourceIdAsString(r: PersistentDiskSamResourceId): String = r.resourceId
  }
  class AppSamResource extends SamResource[AppSamResourceId] {
    def resourceIdAsString(r: AppSamResourceId): String = r.resourceId
    def resourceType(r: AppSamResourceId): SamResourceType = r.resourceType
    def policyNames(r: AppSamResourceId): Set[SamPolicyName] = r.resourceType match {
      case SamResourceType.SharedApp => Set(SamPolicyName.Owner, SamPolicyName.User)
      case _                         => Set(SamPolicyName.Creator, SamPolicyName.Manager)
    }
    def ownerRoleName(r: AppSamResourceId) = r.resourceType match {
      case SamResourceType.SharedApp => SamRole.Owner
      case _                         => SamRole.Creator
    }
  }
  class WorkspaceResource extends SamResource[WorkspaceResourceSamResourceId] {
    def resourceType(r: WorkspaceResourceSamResourceId) = SamResourceType.Workspace
    def policyNames(r: WorkspaceResourceSamResourceId) =
      Set(SamPolicyName.Owner)
    def ownerRoleName(r: WorkspaceResourceSamResourceId) = SamRole.Owner
    def resourceIdAsString(r: WorkspaceResourceSamResourceId): String = r.resourceId
  }
  class WsmResource extends SamResource[WsmResourceSamResourceId] {
    def resourceType(r: WsmResourceSamResourceId) = SamResourceType.WsmResource
    def policyNames(r: WsmResourceSamResourceId) = Set(SamPolicyName.Writer)
    def ownerRoleName(r: WsmResourceSamResourceId) = SamRole.Owner
    def resourceIdAsString(r: WsmResourceSamResourceId): String = r.resourceId
  }

  implicit object ProjectSamResource extends ProjectSamResource
  implicit object RuntimeSamResource extends RuntimeSamResource
  implicit object PersistentDiskSamResource extends PersistentDiskSamResource
  implicit object AppSamResource extends AppSamResource
  implicit object WorkspaceResource extends WorkspaceResource
  implicit object WsmResource extends WsmResource
}

// Typeclass representing an action on a Sam resource
// Constrains at compile time which actions can be checked against which resource types
sealed trait SamResourceAction[R, ActionCategory] extends SamResource[R] {
  def decoder: Decoder[ActionCategory]
  def allActions: List[ActionCategory]
  def cacheableActions: List[ActionCategory]
  def actionAsString(a: ActionCategory): String
}
object SamResourceAction {
  implicit def projectSamResourceAction =
    new ProjectSamResource with SamResourceAction[ProjectSamResourceId, ProjectAction] {
      val decoder = Decoder[ProjectAction]
      val allActions = ProjectAction.allActions.toList
      val cacheableActions = List(ProjectAction.GetRuntimeStatus, ProjectAction.ReadPersistentDisk)
      def actionAsString(a: ProjectAction): String = a.asString
    }

  implicit def runtimeSamResourceAction =
    new RuntimeSamResource with SamResourceAction[RuntimeSamResourceId, RuntimeAction] {
      val decoder = Decoder[RuntimeAction]
      val allActions = RuntimeAction.allActions.toList
      val cacheableActions = List(RuntimeAction.GetRuntimeStatus, RuntimeAction.ConnectToRuntime)
      def actionAsString(a: RuntimeAction): String = a.asString
    }

  implicit def persistentDiskSamResourceAction =
    new PersistentDiskSamResource with SamResourceAction[PersistentDiskSamResourceId, PersistentDiskAction] {
      val decoder = Decoder[PersistentDiskAction]
      val allActions = PersistentDiskAction.allActions.toList
      val cacheableActions = List(PersistentDiskAction.ReadPersistentDisk)
      def actionAsString(a: PersistentDiskAction): String = a.asString
    }

  implicit def AppSamResourceAction = new AppSamResource with SamResourceAction[AppSamResourceId, AppAction] {
    val decoder = Decoder[AppAction]
    val allActions = AppAction.allActions.toList
    val cacheableActions = List(AppAction.GetAppStatus, AppAction.ConnectToApp)
    def actionAsString(a: AppAction): String = a.asString
  }

  implicit def workspaceSamResourceAction =
    new WorkspaceResource with SamResourceAction[WorkspaceResourceSamResourceId, WorkspaceAction] {
      val decoder = Decoder[WorkspaceAction]
      val allActions = WorkspaceAction.allActions.toList
      val cacheableActions =
        List(WorkspaceAction.CreateControlledUserResource, WorkspaceAction.CreateControlledApplicationResource)
      def actionAsString(a: WorkspaceAction): String = a.asString
    }

  implicit def wsmResourceSamResourceAction =
    new WsmResource with SamResourceAction[WsmResourceSamResourceId, WsmResourceAction] {
      val decoder = Decoder[WsmResourceAction]
      val allActions = WsmResourceAction.allActions.toList
      val cacheableActions = List(WsmResourceAction.Write)
      def actionAsString(a: WsmResourceAction): String = a.asString
    }
}

// TODO: https://broadworkbench.atlassian.net/browse/IA-2093 will allow us to remove the *WithProjectFallback methods
trait LeoAuthProvider[F[_]] {
  def serviceAccountProvider: ServiceAccountProvider[F]

  def hasPermission[R, A](samResource: R, action: A, userInfo: UserInfo)(implicit
    sr: SamResourceAction[R, A],
    ev: Ask[F, TraceId]
  ): F[Boolean]

  def hasPermissionWithProjectFallback[R, A](
    samResource: R,
    action: A,
    projectAction: ProjectAction,
    userInfo: UserInfo,
    googleProject: GoogleProject
  )(implicit sr: SamResourceAction[R, A], ev: Ask[F, TraceId]): F[Boolean]

  def getActions[R, A](samResource: R, userInfo: UserInfo)(implicit
    sr: SamResourceAction[R, A],
    ev: Ask[F, TraceId]
  ): F[List[A]]

  def getActionsWithProjectFallback[R, A](samResource: R, googleProject: GoogleProject, userInfo: UserInfo)(implicit
    sr: SamResourceAction[R, A],
    ev: Ask[F, TraceId]
  ): F[(List[A], List[ProjectAction])]

  def filterUserVisible[R](resources: NonEmptyList[R], userInfo: UserInfo)(implicit
    sr: SamResource[R],
    decoder: Decoder[R],
    ev: Ask[F, TraceId]
  ): F[List[R]]

  def filterUserVisibleWithProjectFallback[R](
    resources: NonEmptyList[(GoogleProject, R)],
    userInfo: UserInfo
  )(implicit
    sr: SamResource[R],
    decoder: Decoder[R],
    ev: Ask[F, TraceId]
  ): F[List[(GoogleProject, R)]]

  def filterWorkspaceOwner(
    resources: NonEmptyList[WorkspaceResourceSamResourceId],
    userInfo: UserInfo
  )(implicit
    ev: Ask[F, TraceId]
  ): F[Set[WorkspaceResourceSamResourceId]]

  def filterWorkspaceReader(
    resources: NonEmptyList[WorkspaceResourceSamResourceId],
    userInfo: UserInfo
  )(implicit
    ev: Ask[F, TraceId]
  ): F[Set[WorkspaceResourceSamResourceId]]

  // Creates a resource in Sam
  def notifyResourceCreated[R](samResource: R, creatorEmail: WorkbenchEmail, googleProject: GoogleProject)(implicit
    sr: SamResource[R],
    encoder: Encoder[R],
    ev: Ask[F, TraceId]
  ): F[Unit]

  def notifyResourceCreatedV2[R](samResource: R,
                                 creatorEmail: WorkbenchEmail,
                                 cloudContext: CloudContext,
                                 workspaceId: WorkspaceId,
                                 userInfo: UserInfo
  )(implicit
    sr: SamResource[R],
    encoder: Encoder[R],
    ev: Ask[F, TraceId]
  ): F[Unit]

  // Deletes a resource in Sam
  def notifyResourceDeleted[R](
    samResource: R,
    creatorEmail: WorkbenchEmail,
    googleProject: GoogleProject
  )(implicit sr: SamResource[R], ev: Ask[F, TraceId]): F[Unit]

  def notifyResourceDeletedV2[R](
    samResource: R,
    userInfo: UserInfo
  )(implicit sr: SamResource[R], ev: Ask[F, TraceId]): F[Unit]

  def isUserWorkspaceOwner(
    workspaceResource: WorkspaceResourceSamResourceId,
    userInfo: UserInfo
  )(implicit ev: Ask[F, TraceId]): F[Boolean]

  def isUserWorkspaceReader(
    workspaceResource: WorkspaceResourceSamResourceId,
    userInfo: UserInfo
  )(implicit ev: Ask[F, TraceId]): F[Boolean]

  // Get user info from Sam. If petOrUserInfo is a pet SA, Sam will return it's associated user account info
  def lookupOriginatingUserEmail[R](petOrUserInfo: UserInfo)(implicit ev: Ask[F, TraceId]): F[WorkbenchEmail]

  def checkUserEnabled(petOrUserInfo: UserInfo)(implicit ev: Ask[F, TraceId]): F[Unit]

  def isCustomAppAllowed(userEmail: WorkbenchEmail)(implicit ev: Ask[F, TraceId]): F[Boolean]
}
