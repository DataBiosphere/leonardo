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

sealed trait AuthCheckable[R] {
  def resourceType: SamResourceType
  def actions: Set[LeoAuthAction]
  def cacheableActions: Set[LeoAuthAction]
  def policyNames: Set[AccessPolicyName]
}
object AuthCheckable {
  implicit final case object ProjectAuthCheckable extends AuthCheckable[ProjectSamResource] {
    val resourceType = SamResourceType.Project
    val policyNames = Set(AccessPolicyName.Owner)
    val cacheableActions = Set.empty
    val actions = ProjectAction.allActions.toSet
  }

  implicit final case object RuntimeAuthCheckable extends AuthCheckable[RuntimeSamResource] {
    val resourceType = SamResourceType.Runtime
    val policyNames = Set(AccessPolicyName.Creator)
    val cacheableActions = Set(RuntimeAction.GetRuntimeStatus, RuntimeAction.ConnectToRuntime)
    val actions = RuntimeAction.allActions.toSet
  }

  implicit final case object PersistentDiskAuthCheckable extends AuthCheckable[PersistentDiskSamResource] {
    val resourceType = SamResourceType.PersistentDisk
    val policyNames = Set(AccessPolicyName.Creator)
    val cacheableActions = Set.empty
    val actions = PersistentDiskAction.allActions.toSet
  }

  implicit final case object AppAuthCheckable extends AuthCheckable[AppSamResource] {
    val resourceType = SamResourceType.App
    val policyNames = Set(AccessPolicyName.Creator, AccessPolicyName.Manager)
    val cacheableActions = Set(AppAction.GetAppStatus, AppAction.ConnectToApp)
    val actions = AppAction.allActions.toSet
  }
}

trait LeoAuthProvider[F[_]] {
  def serviceAccountProvider: ServiceAccountProvider[F]

  def hasPermission[R <: SamResource](samResource: R, action: LeoAuthAction, userInfo: UserInfo)(
    implicit authConfig: AuthCheckable[R],
    ev: ApplicativeAsk[F, TraceId]
  ): F[Boolean]

  def hasPermissionWithProjectFallback[R <: SamResource](
    samResource: R,
    action: LeoAuthAction,
    projectAction: ProjectAction,
    userInfo: UserInfo,
    googleProject: GoogleProject
  )(implicit authConfig: AuthCheckable[R], ev: ApplicativeAsk[F, TraceId]): F[Boolean]

  def getActions[R <: SamResource](samResource: R, userInfo: UserInfo)(
    implicit authConfig: AuthCheckable[R],
    ev: ApplicativeAsk[F, TraceId]
  ): F[List[LeoAuthAction]]

  def getActionsWithProjectFallback[R <: SamResource](samResource: R, googleProject: GoogleProject, userInfo: UserInfo)(
    implicit authConfig: AuthCheckable[R],
    ev: ApplicativeAsk[F, TraceId]
  ): F[List[LeoAuthAction]]

  def filterUserVisible[R <: SamResource](resources: List[R], userInfo: UserInfo)(
    implicit authConfig: AuthCheckable[R],
    ev: ApplicativeAsk[F, TraceId]
  ): F[List[R]]

  def filterUserVisibleWithProjectFallback[R <: SamResource](
    resources: List[(GoogleProject, R)],
    userInfo: UserInfo
  )(
    implicit authConfig: AuthCheckable[R],
    ev: ApplicativeAsk[F, TraceId]
  ): F[List[(GoogleProject, R)]]

  // Creates a resource in Sam
  def notifyResourceCreated(samResource: SamResource,
                            creatorEmail: WorkbenchEmail,
                            googleProject: GoogleProject,
                            createManagerPolicy: Boolean = false)(
    implicit ev: ApplicativeAsk[F, TraceId]
  ): F[Unit]

  // Deletes a resource in Sam
  def notifyResourceDeleted(samResource: SamResource,
                            userEmail: WorkbenchEmail,
                            creatorEmail: WorkbenchEmail,
                            googleProject: GoogleProject)(implicit ev: ApplicativeAsk[F, TraceId]): F[Unit]
}
