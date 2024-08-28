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

/**
 * Deprecated: port functionality to SamService, which uses the generated Sam client and models.
 *
 * Typeclass representing a Sam resource and associated policies.
 * @tparam R SamResourceId
 */
@Deprecated
sealed trait SamResource[R] {

  // Values per resource. These require a specific resource instance, which affects return value for some App types.
  /** Type of the resource (originally defined in Sam). */
  def resourceType(r: R): SamResourceType

  /** ID of the resource. */
  def resourceIdAsString(r: R): String

  /**
   * Sam roles on which read access is explicitly granted. Empty set permits all.
   * Given a resource type `X`, with `someX` a child of a workspace or project W,
   * a user could read someX iff:
   * 1. the user is granted Owner role or its equivalent on W
   * 2. the user is granted any role in X.policyNames
   * 3. X.policyNames is empty and the user is granted any role on W
   */
  def policyNames(r: R): Set[SamPolicyName]

  /** Sam role corresponding to Leonardo's concept of "owner" for the resource. */
  def ownerRoleName(r: R): SamRole

  // Defaults. These are independent of a specific resource instance. Equivalent to the method calls above except for some App types.
  def resourceType: SamResourceType
  def policyNames: Set[SamPolicyName]
  def ownerRoleName: SamRole
}
object SamResource {
  class ProjectSamResource extends SamResource[ProjectSamResourceId] {
    val resourceType: SamResourceType = SamResourceType.Project
    val policyNames: Set[SamPolicyName] = Set.empty
    val ownerRoleName: SamRole = SamRole.Owner

    def resourceType(r: ProjectSamResourceId): SamResourceType = resourceType
    def policyNames(r: ProjectSamResourceId): Set[SamPolicyName] = policyNames
    def ownerRoleName(r: ProjectSamResourceId): SamRole = ownerRoleName
    def resourceIdAsString(r: ProjectSamResourceId): String = r.googleProject.value
  }
  class RuntimeSamResource extends SamResource[RuntimeSamResourceId] {
    val resourceType: SamResourceType = SamResourceType.Runtime
    val policyNames: Set[SamPolicyName] = Set(SamPolicyName.Creator)
    val ownerRoleName: SamRole = SamRole.Creator

    def resourceType(r: RuntimeSamResourceId): SamResourceType = resourceType
    def policyNames(r: RuntimeSamResourceId): Set[SamPolicyName] = policyNames
    def ownerRoleName(r: RuntimeSamResourceId): SamRole = ownerRoleName
    def resourceIdAsString(r: RuntimeSamResourceId): String = r.resourceId
  }
  class PersistentDiskSamResource extends SamResource[PersistentDiskSamResourceId] {
    val resourceType: SamResourceType = SamResourceType.PersistentDisk
    val policyNames: Set[SamPolicyName] = Set(SamPolicyName.Creator)
    val ownerRoleName: SamRole = SamRole.Creator

    def resourceType(r: PersistentDiskSamResourceId): SamResourceType = resourceType
    def policyNames(r: PersistentDiskSamResourceId): Set[SamPolicyName] = policyNames
    def ownerRoleName(r: PersistentDiskSamResourceId): SamRole = ownerRoleName
    def resourceIdAsString(r: PersistentDiskSamResourceId): String = r.resourceId
  }
  class AppSamResource extends SamResource[AppSamResourceId] {
    // defaults apply to App
    val resourceType: SamResourceType = SamResourceType.App
    val policyNames: Set[SamPolicyName] = Set(SamPolicyName.Creator, SamPolicyName.Manager)
    val ownerRoleName: SamRole = SamRole.Creator

    def resourceIdAsString(r: AppSamResourceId): String = r.resourceId
    def resourceType(r: AppSamResourceId): SamResourceType = r.resourceType
    def policyNames(r: AppSamResourceId): Set[SamPolicyName] = r.resourceType match {
      case SamResourceType.SharedApp => Set(SamPolicyName.Owner, SamPolicyName.User)
      case _                         => policyNames
    }
    def ownerRoleName(r: AppSamResourceId): SamRole = r.resourceType match {
      case SamResourceType.SharedApp => SamRole.Owner
      case _                         => ownerRoleName
    }
  }
  class WorkspaceResource extends SamResource[WorkspaceResourceSamResourceId] {
    val resourceType: SamResourceType = SamResourceType.Workspace
    val policyNames: Set[SamPolicyName] = Set.empty
    val ownerRoleName: SamRole = SamRole.Owner

    def resourceType(r: WorkspaceResourceSamResourceId): SamResourceType = resourceType
    def policyNames(r: WorkspaceResourceSamResourceId): Set[SamPolicyName] =
      policyNames
    def ownerRoleName(r: WorkspaceResourceSamResourceId): SamRole = ownerRoleName
    def resourceIdAsString(r: WorkspaceResourceSamResourceId): String = r.resourceId
  }
  class WsmResource extends SamResource[WsmResourceSamResourceId] {
    val resourceType: SamResourceType = SamResourceType.WsmResource
    val policyNames: Set[SamPolicyName] = Set(SamPolicyName.Writer)
    val ownerRoleName: SamRole = SamRole.Owner

    def resourceType(r: WsmResourceSamResourceId): SamResourceType = resourceType
    def policyNames(r: WsmResourceSamResourceId): Set[SamPolicyName] = policyNames
    def ownerRoleName(r: WsmResourceSamResourceId): SamRole = ownerRoleName
    def resourceIdAsString(r: WsmResourceSamResourceId): String = r.resourceId
  }

  implicit object ProjectSamResource extends ProjectSamResource
  implicit object RuntimeSamResource extends RuntimeSamResource
  implicit object PersistentDiskSamResource extends PersistentDiskSamResource
  implicit object AppSamResource extends AppSamResource
  implicit object WorkspaceResource extends WorkspaceResource
  implicit object WsmResource extends WsmResource
}

/**
 * Deprecated: port functionality to SamService, which uses the generated Sam client and models.
 *
 * Typeclass representing an action on a Sam resource
 * Constrains at compile time which actions can be checked against which resource types
 */
@Deprecated
sealed trait SamResourceAction[R, ActionCategory] extends SamResource[R] {
  def decoder: Decoder[ActionCategory]
  def allActions: List[ActionCategory]
  def cacheableActions: List[ActionCategory]
  def actionAsString(a: ActionCategory): String
}
object SamResourceAction {
  implicit def projectSamResourceAction
    : ProjectSamResource with SamResourceAction[ProjectSamResourceId, ProjectAction] =
    new ProjectSamResource with SamResourceAction[ProjectSamResourceId, ProjectAction] {
      val decoder = Decoder[ProjectAction]
      val allActions = ProjectAction.allActions.toList
      val cacheableActions = List(ProjectAction.GetRuntimeStatus, ProjectAction.ReadPersistentDisk)

      def actionAsString(a: ProjectAction): String = a.asString
    }

  implicit def runtimeSamResourceAction
    : RuntimeSamResource with SamResourceAction[RuntimeSamResourceId, RuntimeAction] =
    new RuntimeSamResource with SamResourceAction[RuntimeSamResourceId, RuntimeAction] {
      val decoder = Decoder[RuntimeAction]
      val allActions = RuntimeAction.allActions.toList
      val cacheableActions = List(RuntimeAction.GetRuntimeStatus, RuntimeAction.ConnectToRuntime)

      def actionAsString(a: RuntimeAction): String = a.asString
    }

  implicit def persistentDiskSamResourceAction
    : PersistentDiskSamResource with SamResourceAction[PersistentDiskSamResourceId, PersistentDiskAction] =
    new PersistentDiskSamResource with SamResourceAction[PersistentDiskSamResourceId, PersistentDiskAction] {
      val decoder = Decoder[PersistentDiskAction]
      val allActions = PersistentDiskAction.allActions.toList
      val cacheableActions = List(PersistentDiskAction.ReadPersistentDisk)

      def actionAsString(a: PersistentDiskAction): String = a.asString
    }

  implicit def AppSamResourceAction: AppSamResource with SamResourceAction[AppSamResourceId, AppAction] =
    new AppSamResource with SamResourceAction[AppSamResourceId, AppAction] {
      val decoder = Decoder[AppAction]
      val allActions = AppAction.allActions.toList
      val cacheableActions = List(AppAction.GetAppStatus, AppAction.ConnectToApp)

      def actionAsString(a: AppAction): String = a.asString
    }

  implicit def workspaceSamResourceAction
    : WorkspaceResource with SamResourceAction[WorkspaceResourceSamResourceId, WorkspaceAction] =
    new WorkspaceResource with SamResourceAction[WorkspaceResourceSamResourceId, WorkspaceAction] {
      val decoder = Decoder[WorkspaceAction]
      val allActions = WorkspaceAction.allActions.toList
      val cacheableActions =
        List(WorkspaceAction.CreateControlledUserResource, WorkspaceAction.CreateControlledApplicationResource)
      def actionAsString(a: WorkspaceAction): String = a.asString
    }

  implicit def wsmResourceSamResourceAction
    : WsmResource with SamResourceAction[WsmResourceSamResourceId, WsmResourceAction] =
    new WsmResource with SamResourceAction[WsmResourceSamResourceId, WsmResourceAction] {
      val decoder = Decoder[WsmResourceAction]
      val allActions = WsmResourceAction.allActions.toList
      val cacheableActions = List(WsmResourceAction.Write)
      def actionAsString(a: WsmResourceAction): String = a.asString
    }
}

/**
 * Deprecated: port functionality to SamService, which uses the generated Sam client and models.
 */
@Deprecated
trait LeoAuthProvider[F[_]] {
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

  def listResourceIds[R <: SamResourceId](
    hasOwnerRole: Boolean,
    userInfo: UserInfo
  )(implicit
    resourceDefinition: SamResource[R],
    appDefinition: SamResource[AppSamResourceId],
    resourceIdDecoder: Decoder[R],
    ev: Ask[F, TraceId]
  ): F[Set[R]]

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

  def filterResourceProjectVisible[R](
    resources: NonEmptyList[(GoogleProject, R)],
    userInfo: UserInfo
  )(implicit
    sr: SamResource[R],
    decoder: Decoder[R],
    ev: Ask[F, TraceId]
  ): F[List[(GoogleProject, R)]]

  def isUserProjectReader(
    cloudContext: CloudContext,
    userInfo: UserInfo
  )(implicit ev: Ask[F, TraceId]): F[Boolean]

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
  def isSasAppAllowed(userEmail: WorkbenchEmail)(implicit ev: Ask[F, TraceId]): F[Boolean]

  def isAdminUser(userInfo: UserInfo)(implicit ev: Ask[F, TraceId]): F[Boolean]

  def getLeoAuthToken: F[String]
}
