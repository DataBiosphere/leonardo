package org.broadinstitute.dsde.workbench.leonardo
package model

import ca.mrvisser.sealerate
import cats.mtl.ApplicativeAsk
import org.broadinstitute.dsde.workbench.leonardo.SamResource.{PersistentDiskSamResource, RuntimeSamResource}
import org.broadinstitute.dsde.workbench.model.google.GoogleProject
import org.broadinstitute.dsde.workbench.model.{TraceId, UserInfo, WorkbenchEmail}

sealed trait LeoAuthAction extends Product with Serializable

sealed trait ProjectAction extends LeoAuthAction
object ProjectAction {
  case object CreateRuntime extends ProjectAction
  case object CreatePersistentDisk extends ProjectAction
  val allActions = sealerate.values[ProjectAction]
}

sealed trait RuntimeAction extends LeoAuthAction
object RuntimeAction {
  case object GetRuntimeStatus extends RuntimeAction
  case object ConnectToRuntime extends RuntimeAction
  case object SyncDataToRuntime extends RuntimeAction
  case object DeleteRuntime extends RuntimeAction
  case object ModifyRuntime extends RuntimeAction
  case object StopStartRuntime extends RuntimeAction
  val allActions = sealerate.values[RuntimeAction]
  val projectFallbackIneligibleActions: Set[RuntimeAction] = Set(ConnectToRuntime)
}

sealed trait PersistentDiskAction extends LeoAuthAction
object PersistentDiskAction {
  case object ReadPersistentDisk extends PersistentDiskAction
  case object AttachPersistentDisk extends PersistentDiskAction
  case object ModifyPersistentDisk extends PersistentDiskAction
  case object DeletePersistentDisk extends PersistentDiskAction
  val allActions = sealerate.values[PersistentDiskAction]
  val projectFallbackIneligibleActions: Set[PersistentDiskAction] =
    Set(ReadPersistentDisk, AttachPersistentDisk, ModifyPersistentDisk)
}

trait LeoAuthProvider[F[_]] {
  def serviceAccountProvider: ServiceAccountProvider[F]

  def hasProjectPermission(userInfo: UserInfo, action: ProjectAction, googleProject: GoogleProject)(
    implicit ev: ApplicativeAsk[F, TraceId]
  ): F[Boolean]

  def hasRuntimePermission(samResource: RuntimeSamResource,
                           userInfo: UserInfo,
                           action: RuntimeAction,
                           googleProject: GoogleProject)(implicit ev: ApplicativeAsk[F, TraceId]): F[Boolean]

  def hasPersistentDiskPermission(samResource: PersistentDiskSamResource,
                                  userInfo: UserInfo,
                                  action: PersistentDiskAction,
                                  googleProject: GoogleProject)(implicit ev: ApplicativeAsk[F, TraceId]): F[Boolean]

  def filterUserVisibleRuntimes(userInfo: UserInfo, runtimes: List[(GoogleProject, RuntimeSamResource)])(
    implicit ev: ApplicativeAsk[F, TraceId]
  ): F[List[(GoogleProject, RuntimeSamResource)]]

  def filterUserVisiblePersistentDisks(userInfo: UserInfo, disks: List[(GoogleProject, PersistentDiskSamResource)])(
    implicit ev: ApplicativeAsk[F, TraceId]
  ): F[List[(GoogleProject, PersistentDiskSamResource)]]

  //Notifications that Leo has created/destroyed resources. Allows the auth provider to register things.

  def notifyResourceCreated(samResource: SamResource, creatorEmail: WorkbenchEmail, googleProject: GoogleProject)(
    implicit ev: ApplicativeAsk[F, TraceId]
  ): F[Unit]

  def notifyResourceDeleted(samResource: SamResource,
                            userEmail: WorkbenchEmail,
                            creatorEmail: WorkbenchEmail,
                            googleProject: GoogleProject)(implicit ev: ApplicativeAsk[F, TraceId]): F[Unit]
}
