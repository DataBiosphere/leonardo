package org.broadinstitute.dsde.workbench.leonardo
package model

import cats.mtl.ApplicativeAsk
import org.broadinstitute.dsde.workbench.leonardo.SamResource.{
  AppSamResource,
  PersistentDiskSamResource,
  ProjectSamResource,
  RuntimeSamResource
}
import org.broadinstitute.dsde.workbench.model.google.GoogleProject
import org.broadinstitute.dsde.workbench.model.{TraceId, UserInfo, WorkbenchEmail}

// Typeclass encapsulating which actions can be checked against which Sam resource types
sealed trait ActionCheckable[R] {
  type ActionCategory <: LeoAuthAction
  def allActions: List[ActionCategory]
  def cacheableActions: List[ActionCategory]
}
object ActionCheckable {
  implicit def projectActionCheckable[A <: ProjectAction] = new ActionCheckable[ProjectSamResource] {
    type ActionCategory = ProjectAction
    val allActions = ProjectAction.allActions.toList
    val cacheableActions = List(ProjectAction.GetRuntimeStatus, ProjectAction.ReadPersistentDisk)
  }

  implicit def runtimeActionCheckable[A <: RuntimeAction] = new ActionCheckable[RuntimeSamResource] {
    type ActionCategory = RuntimeAction
    val allActions = RuntimeAction.allActions.toList
    val cacheableActions = List(RuntimeAction.GetRuntimeStatus, RuntimeAction.ConnectToRuntime)
  }

  implicit def persistentDiskActionCheckable[A <: PersistentDiskAction] =
    new ActionCheckable[PersistentDiskSamResource] {
      type ActionCategory = PersistentDiskAction
      val allActions = PersistentDiskAction.allActions.toList
      val cacheableActions = List(PersistentDiskAction.ReadPersistentDisk)
    }

  implicit def appActionCheckable[A <: AppAction] = new ActionCheckable[AppSamResource] {
    type ActionCategory = AppAction
    val allActions = AppAction.allActions.toList
    val cacheableActions = List(AppAction.GetAppStatus, AppAction.ConnectToApp)
  }
}

// Typeclass encapsulating which policies are used for which Sam resource types
sealed trait PolicyCheckable[R] {
  def resourceType: SamResourceType
  def policyNames: Set[AccessPolicyName]
}
object PolicyCheckable {
  implicit def projectPolicyCheckable = new PolicyCheckable[ProjectSamResource] {
    val resourceType = SamResourceType.Project
    val policyNames = Set(AccessPolicyName.Owner)
  }
  implicit def runtimePolicyCheckable = new PolicyCheckable[RuntimeSamResource] {
    val resourceType = SamResourceType.Runtime
    val policyNames = Set(AccessPolicyName.Creator)
  }
  implicit def persistentDiskPolicyCheckable = new PolicyCheckable[PersistentDiskSamResource] {
    val resourceType = SamResourceType.PersistentDisk
    val policyNames = Set(AccessPolicyName.Creator)
  }
  implicit def appPolicyCheckable = new PolicyCheckable[AppSamResource] {
    val resourceType = SamResourceType.App
    val policyNames = Set(AccessPolicyName.Creator, AccessPolicyName.Manager)
  }
}

// TODO: IA-XYZ will allow us to remove the *WithProjectFallback methods and therefore reduce the
// number of Sam calls.
trait LeoAuthProvider[F[_]] {
  def serviceAccountProvider: ServiceAccountProvider[F]

  def hasPermission[R <: SamResource, A <: LeoAuthAction](samResource: R, action: A, userInfo: UserInfo)(
    implicit act: ActionCheckable[R],
    ev: ApplicativeAsk[F, TraceId]
  ): F[Boolean]

  def hasPermissionWithProjectFallback[R <: SamResource, A <: LeoAuthAction](
    samResource: R,
    action: A,
    projectAction: ProjectAction,
    userInfo: UserInfo,
    googleProject: GoogleProject
  )(implicit act: ActionCheckable[R], ev: ApplicativeAsk[F, TraceId]): F[Boolean]

  def getActions[R <: SamResource, A <: LeoAuthAction](samResource: R, userInfo: UserInfo)(
    implicit act: ActionCheckable[R],
    ev: ApplicativeAsk[F, TraceId]
  ): F[List[act.ActionCategory]]

  def getActionsWithProjectFallback[R <: SamResource, A <: LeoAuthAction](samResource: R,
                                                                          googleProject: GoogleProject,
                                                                          userInfo: UserInfo)(
    implicit act: ActionCheckable[R],
    ev: ApplicativeAsk[F, TraceId]
  ): F[List[LeoAuthAction]]

  def filterUserVisible[R <: SamResource](resources: List[R], userInfo: UserInfo)(
    implicit pol: PolicyCheckable[R],
    ev: ApplicativeAsk[F, TraceId]
  ): F[List[R]]

  def filterUserVisibleWithProjectFallback[R <: SamResource](
    resources: List[(GoogleProject, R)],
    userInfo: UserInfo
  )(
    implicit pol: PolicyCheckable[R],
    ev: ApplicativeAsk[F, TraceId]
  ): F[List[(GoogleProject, R)]]

  // Creates a resource in Sam
  def notifyResourceCreated[R <: SamResource](samResource: R,
                                              creatorEmail: WorkbenchEmail,
                                              googleProject: GoogleProject)(
    implicit pol: PolicyCheckable[R],
    ev: ApplicativeAsk[F, TraceId]
  ): F[Unit]

  // Deletes a resource in Sam
  def notifyResourceDeleted[R <: SamResource](
    samResource: R,
    userEmail: WorkbenchEmail,
    creatorEmail: WorkbenchEmail,
    googleProject: GoogleProject
  )(implicit pol: PolicyCheckable[R], ev: ApplicativeAsk[F, TraceId]): F[Unit]
}
