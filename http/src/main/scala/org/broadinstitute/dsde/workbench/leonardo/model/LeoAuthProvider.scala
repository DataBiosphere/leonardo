package org.broadinstitute.dsde.workbench.leonardo
package model

import cats.mtl.ApplicativeAsk
import io.circe.Decoder
import org.broadinstitute.dsde.workbench.leonardo.SamResource.{
  AppSamResource,
  PersistentDiskSamResource,
  ProjectSamResource,
  RuntimeSamResource
}
import org.broadinstitute.dsde.workbench.leonardo.SamResourcePolicy.{
  SamAppPolicy,
  SamPersistentDiskPolicy,
  SamProjectPolicy,
  SamRuntimePolicy
}
import org.broadinstitute.dsde.workbench.leonardo.dao.HttpSamDAO._
import org.broadinstitute.dsde.workbench.model.google.GoogleProject
import org.broadinstitute.dsde.workbench.model.{TraceId, UserInfo, WorkbenchEmail}

sealed trait AuthCheckable[R] {
  def resourceType: SamResourceType
  def actions: Set[LeoAuthAction]
  def cacheableActions: Set[LeoAuthAction]
  def policyNames: Set[AccessPolicyName]
  type Policy <: SamResourcePolicy
  def policyDecoder: Decoder[Policy]
}
object AuthCheckable {

  implicit final case object ProjectAuthCheckable extends AuthCheckable[ProjectSamResource] {
    val resourceType = SamResourceType.Project
    val policyNames = Set(AccessPolicyName.Owner)
    val cacheableActions = Set.empty
    val actions = ProjectAction.allActions.toSet
    type Policy = SamProjectPolicy
    def policyDecoder = implicitly[Decoder[Policy]]
  }

  implicit final case object RuntimeAuthCheckable extends AuthCheckable[RuntimeSamResource] {
    val resourceType = SamResourceType.Runtime
    val policyNames = Set(AccessPolicyName.Creator)
    val cacheableActions = Set(RuntimeAction.ConnectToRuntime)
    val actions = RuntimeAction.allActions.toSet
    type Policy = SamRuntimePolicy
    def policyDecoder = implicitly[Decoder[Policy]]
  }

  implicit final case object PersistentDiskAuthCheckable extends AuthCheckable[PersistentDiskSamResource] {
    val resourceType = SamResourceType.PersistentDisk
    val policyNames = Set(AccessPolicyName.Creator)
    val cacheableActions = Set.empty
    val actions = PersistentDiskAction.allActions.toSet
    type Policy = SamPersistentDiskPolicy
    def policyDecoder = implicitly[Decoder[Policy]]
  }

  implicit final case object AppAuthCheckable extends AuthCheckable[AppSamResource] {
    val resourceType = SamResourceType.App
    val policyNames = Set(AccessPolicyName.Creator, AccessPolicyName.Manager)
    val cacheableActions = Set(AppAction.ConnectToApp)
    val actions = AppAction.allActions.toSet
    type Policy = SamAppPolicy
    def policyDecoder = implicitly[Decoder[Policy]]
  }

}

trait LeoAuthProvider[F[_]] {
  def serviceAccountProvider: ServiceAccountProvider[F]

  def hasPermission[R <: SamResource](samResource: R, action: LeoAuthAction, userInfo: UserInfo)(
    implicit ev: ApplicativeAsk[F, TraceId],
    ev2: AuthCheckable[R]
  ): F[Boolean]

  def hasPermissionWithProjectFallback[R <: SamResource](
    samResource: R,
    action: LeoAuthAction,
    projectAction: ProjectAction,
    userInfo: UserInfo,
    googleProject: GoogleProject
  )(implicit ev: ApplicativeAsk[F, TraceId], ev2: AuthCheckable[R]): F[Boolean]

  def getActions[R <: SamResource](samResource: R, userInfo: UserInfo)(
    implicit ev: ApplicativeAsk[F, TraceId],
    ev2: AuthCheckable[R]
  ): F[List[LeoAuthAction]]

  def getActionsWithProjectFallback[R <: SamResource](samResource: R, googleProject: GoogleProject, userInfo: UserInfo)(
    implicit ev: ApplicativeAsk[F, TraceId],
    ev2: AuthCheckable[R]
  ): F[List[LeoAuthAction]]

  def filterUserVisible[R <: SamResource](resources: List[R], userInfo: UserInfo)(
    implicit ev: ApplicativeAsk[F, TraceId],
    ev2: AuthCheckable[R]
  ): F[List[R]]

  def filterUserVisibleWithProjectFallback[R <: SamResource](
    resources: List[(GoogleProject, R)],
    userInfo: UserInfo
  )(
    implicit ev: ApplicativeAsk[F, TraceId],
    ev2: AuthCheckable[R]
  ): F[List[(GoogleProject, R)]]

  // TODO need other create method to attach multiple policies?

  // Creates a resource in Sam
  def notifyResourceCreated(samResource: SamResource, creatorEmail: WorkbenchEmail, googleProject: GoogleProject)(
    implicit ev: ApplicativeAsk[F, TraceId]
  ): F[Unit]

  // Deletes a resource in Sam
  def notifyResourceDeleted(samResource: SamResource,
                            userEmail: WorkbenchEmail,
                            creatorEmail: WorkbenchEmail,
                            googleProject: GoogleProject)(implicit ev: ApplicativeAsk[F, TraceId]): F[Unit]
}
