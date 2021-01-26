package org.broadinstitute.dsde.workbench.leonardo
package algebra

import cats.mtl.Ask
import io.circe.{Decoder, Encoder}
import org.broadinstitute.dsde.workbench.leonardo.algebra.SamResource._
import org.broadinstitute.dsde.workbench.model.google.GoogleProject
import org.broadinstitute.dsde.workbench.leonardo.algebra.HttpSamDAO._
import org.broadinstitute.dsde.workbench.model.{TraceId, WorkbenchEmail}
import org.broadinstitute.dsde.workbench.util.health.StatusCheckResponse
import org.http4s.headers.Authorization

trait SamDAO[F[_]] {
  def getStatus(implicit ev: Ask[F, TraceId]): F[StatusCheckResponse]

  def hasResourcePermission[R, A](resource: R, action: A, authHeader: Authorization)(
    implicit sr: SamResourceAction[R, A],
    ev: Ask[F, TraceId]
  ): F[Boolean] =
    hasResourcePermissionUnchecked(sr.resourceType,
                                   sr.resourceIdAsString(resource),
                                   sr.actionAsString(action),
                                   authHeader)

  // This exists because of guava cache
  private[leonardo] def hasResourcePermissionUnchecked(resourceType: SamResourceType,
                                                       resource: String,
                                                       action: String,
                                                       authHeader: Authorization)(
    implicit ev: Ask[F, TraceId]
  ): F[Boolean]

  def getResourcePolicies[R](authHeader: Authorization)(
    implicit sr: SamResource[R],
    decoder: Decoder[R],
    ev: Ask[F, TraceId]
  ): F[List[(R, SamPolicyName)]]

  def createResource[R](resource: R, creatorEmail: WorkbenchEmail, googleProject: GoogleProject)(
    implicit sr: SamResource[R],
    ev: Ask[F, TraceId]
  ): F[Unit]

  def createResourceWithManagerPolicy[R](resource: R, creatorEmail: WorkbenchEmail, googleProject: GoogleProject)(
    implicit sr: SamResource[R],
    encoder: Encoder[R],
    ev: Ask[F, TraceId]
  ): F[Unit]

  def deleteResource[R](resource: R, creatorEmail: WorkbenchEmail, googleProject: GoogleProject)(
    implicit sr: SamResource[R],
    ev: Ask[F, TraceId]
  ): F[Unit]

  def getListOfResourcePermissions[R, A](resource: R, authHeader: Authorization)(
    implicit sr: SamResourceAction[R, A],
    ev: Ask[F, TraceId]
  ): F[List[sr.ActionCategory]]

  def getPetServiceAccount(authorization: Authorization, googleProject: GoogleProject)(
    implicit ev: Ask[F, TraceId]
  ): F[Option[WorkbenchEmail]]

  def getUserProxy(userEmail: WorkbenchEmail)(implicit ev: Ask[F, TraceId]): F[Option[WorkbenchEmail]]

  def getCachedPetAccessToken(userEmail: WorkbenchEmail, googleProject: GoogleProject)(
    implicit ev: Ask[F, TraceId]
  ): F[Option[String]]

  def getUserSubjectId(userEmail: WorkbenchEmail, googleProject: GoogleProject)(
    implicit ev: Ask[F, TraceId]
  ): F[Option[UserSubjectId]]
}

final case class UserSubjectId(asString: String) extends AnyVal

// Typeclass representing a Sam resource and associated policies
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

// Typeclass representing an action on a Sam resource
// Constrains at compile time which actions can be checked against which resource types
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
      val decoder = Decoder[ProjectAction]
      val allActions = ProjectAction.allActions.toList
      val cacheableActions = List(ProjectAction.GetRuntimeStatus, ProjectAction.ReadPersistentDisk)
      def actionAsString(a: A): String = a.asString
    }

  implicit def runtimeSamResourceAction[A <: RuntimeAction] =
    new RuntimeSamResource with SamResourceAction[RuntimeSamResourceId, A] {
      type ActionCategory = RuntimeAction
      val decoder = Decoder[RuntimeAction]
      val allActions = RuntimeAction.allActions.toList
      val cacheableActions = List(RuntimeAction.GetRuntimeStatus, RuntimeAction.ConnectToRuntime)
      def actionAsString(a: A): String = a.asString
    }

  implicit def persistentDiskSamResourceAction[A <: PersistentDiskAction] =
    new PersistentDiskSamResource with SamResourceAction[PersistentDiskSamResourceId, A] {
      type ActionCategory = PersistentDiskAction
      val decoder = Decoder[PersistentDiskAction]
      val allActions = PersistentDiskAction.allActions.toList
      val cacheableActions = List(PersistentDiskAction.ReadPersistentDisk)
      def actionAsString(a: A): String = a.asString
    }

  implicit def AppSamResourceAction[A <: AppAction] = new AppSamResource with SamResourceAction[AppSamResourceId, A] {
    type ActionCategory = AppAction
    val decoder = Decoder[AppAction]
    val allActions = AppAction.allActions.toList
    val cacheableActions = List(AppAction.GetAppStatus, AppAction.ConnectToApp)
    def actionAsString(a: A): String = a.asString
  }
}
