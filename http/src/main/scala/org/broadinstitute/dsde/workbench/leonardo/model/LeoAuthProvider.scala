package org.broadinstitute.dsde.workbench.leonardo
package model

import cats.mtl.ApplicativeAsk
import io.circe.{Decoder, Encoder}
import org.broadinstitute.dsde.workbench.leonardo.model.SamResource.{
  AppSamResource,
  PersistentDiskSamResource,
  ProjectSamResource,
  RuntimeSamResource
}
import org.broadinstitute.dsde.workbench.leonardo.dao.HttpSamDAO._
import org.broadinstitute.dsde.workbench.model.google.GoogleProject
import org.broadinstitute.dsde.workbench.model.{TraceId, UserInfo, WorkbenchEmail}

sealed trait SamResource[R] {
  def resourceType: SamResourceType
  def resourceIdAsString(r: R): String
  def policyNames: Set[SamPolicyName]
}
object SamResource {
  class ProjectSamResource extends SamResource[ProjectSamResourceId] {
    val resourceType = SamResourceType.Project
    val policyNames = Set(SamPolicyName.Owner)
    def resourceIdAsString(r: ProjectSamResourceId): String = r.googleProject.value
  }
  class RuntimeSamResource extends SamResource[RuntimeSamResourceId] {
    val resourceType = SamResourceType.Runtime
    val policyNames = Set(SamPolicyName.Creator)
    def resourceIdAsString(r: RuntimeSamResourceId): String = r.resourceId
  }
  class PersistentDiskSamResource extends SamResource[PersistentDiskSamResourceId] {
    val resourceType = SamResourceType.PersistentDisk
    val policyNames = Set(SamPolicyName.Creator)
    def resourceIdAsString(r: PersistentDiskSamResourceId): String = r.resourceId
  }
  class AppSamResource extends SamResource[AppSamResourceId] {
    val resourceType = SamResourceType.App
    val policyNames = Set(SamPolicyName.Creator, SamPolicyName.Manager)
    def resourceIdAsString(r: AppSamResourceId): String = r.resourceId
  }

  implicit object ProjectSamResource extends ProjectSamResource
  implicit object RuntimeSamResource extends RuntimeSamResource
  implicit object PersistentDiskSamResource extends PersistentDiskSamResource
  implicit object AppSamResource extends AppSamResource
}

sealed trait SamResourceAction[R, A] extends SamResource[R] {
  type ActionCategory
  def decoder: Decoder[ActionCategory]
  def allActions: List[ActionCategory]
  def cacheableActions: List[ActionCategory]
  def actionAsString(a: A): String
}
object SamResourceAction {
  implicit def projectSamResourceAction[A <: ProjectAction] =
    new ProjectSamResource with SamResourceAction[ProjectSamResourceId, A] {
      type ActionCategory = ProjectAction
      val decoder = implicitly[Decoder[ProjectAction]]
      val allActions = ProjectAction.allActions.toList
      val cacheableActions = List(ProjectAction.GetRuntimeStatus, ProjectAction.ReadPersistentDisk)
      def actionAsString(a: A): String = a.asString
    }

  implicit def runtimeSamResourceAction[A <: RuntimeAction] =
    new RuntimeSamResource with SamResourceAction[RuntimeSamResourceId, A] {
      type ActionCategory = RuntimeAction
      val decoder = implicitly[Decoder[RuntimeAction]]
      val allActions = RuntimeAction.allActions.toList
      val cacheableActions = List(RuntimeAction.GetRuntimeStatus, RuntimeAction.ConnectToRuntime)
      def actionAsString(a: A): String = a.asString
    }

  implicit def persistentDiskSamResourceAction[A <: PersistentDiskAction] =
    new PersistentDiskSamResource with SamResourceAction[PersistentDiskSamResourceId, A] {
      type ActionCategory = PersistentDiskAction
      val decoder = implicitly[Decoder[PersistentDiskAction]]
      val allActions = PersistentDiskAction.allActions.toList
      val cacheableActions = List(PersistentDiskAction.ReadPersistentDisk)
      def actionAsString(a: A): String = a.asString
    }

  implicit def AppSamResourceAction[A <: AppAction] = new AppSamResource with SamResourceAction[AppSamResourceId, A] {
    type ActionCategory = AppAction
    val decoder = implicitly[Decoder[AppAction]]
    val allActions = AppAction.allActions.toList
    val cacheableActions = List(AppAction.GetAppStatus, AppAction.ConnectToApp)
    def actionAsString(a: A): String = a.asString
  }
}

// TODO: IA-XYZ will allow us to remove the *WithProjectFallback methods and
// therefore reduce the number of Sam calls.
trait LeoAuthProvider[F[_]] {
  def serviceAccountProvider: ServiceAccountProvider[F]

  def hasPermission[R, A](samResource: R, action: A, userInfo: UserInfo)(
    implicit sr: SamResourceAction[R, A],
    ev: ApplicativeAsk[F, TraceId]
  ): F[Boolean]

  def hasPermissionWithProjectFallback[R, A](
    samResource: R,
    action: A,
    projectAction: ProjectAction,
    userInfo: UserInfo,
    googleProject: GoogleProject
  )(implicit sr: SamResourceAction[R, A], ev: ApplicativeAsk[F, TraceId]): F[Boolean]

  def getActions[R, A](samResource: R, userInfo: UserInfo)(
    implicit sr: SamResourceAction[R, A],
    ev: ApplicativeAsk[F, TraceId]
  ): F[List[sr.ActionCategory]]

  def getActionsWithProjectFallback[R, A](samResource: R, googleProject: GoogleProject, userInfo: UserInfo)(
    implicit sr: SamResourceAction[R, A],
    ev: ApplicativeAsk[F, TraceId]
  ): F[(List[sr.ActionCategory], List[ProjectAction])]

  def filterUserVisible[R](resources: List[R], userInfo: UserInfo)(
    implicit sr: SamResource[R],
    decoder: Decoder[R],
    ev: ApplicativeAsk[F, TraceId]
  ): F[List[R]]

  def filterUserVisibleWithProjectFallback[R](
    resources: List[(GoogleProject, R)],
    userInfo: UserInfo
  )(
    implicit sr: SamResource[R],
    decoder: Decoder[R],
    ev: ApplicativeAsk[F, TraceId]
  ): F[List[(GoogleProject, R)]]

  // Creates a resource in Sam
  def notifyResourceCreated[R](samResource: R, creatorEmail: WorkbenchEmail, googleProject: GoogleProject)(
    implicit sr: SamResource[R],
    encoder: Encoder[R],
    ev: ApplicativeAsk[F, TraceId]
  ): F[Unit]

  // Deletes a resource in Sam
  def notifyResourceDeleted[R](
    samResource: R,
    creatorEmail: WorkbenchEmail,
    googleProject: GoogleProject
  )(implicit sr: SamResource[R], ev: ApplicativeAsk[F, TraceId]): F[Unit]
}
