package org.broadinstitute.dsde.workbench.leonardo
package auth

import cats.effect.IO
import cats.mtl.ApplicativeAsk
import com.typesafe.config.Config
import net.ceedubs.ficus.Ficus._
import org.broadinstitute.dsde.workbench.leonardo.SamResource.{PersistentDiskSamResource, RuntimeSamResource}
import org.broadinstitute.dsde.workbench.leonardo.model._
import org.broadinstitute.dsde.workbench.model.google.GoogleProject
import org.broadinstitute.dsde.workbench.model.{TraceId, UserInfo, WorkbenchEmail}

class MockLeoAuthProvider(authConfig: Config, saProvider: ServiceAccountProvider[IO], notifySucceeds: Boolean = true)
    extends LeoAuthProvider[IO] {
  override def serviceAccountProvider: ServiceAccountProvider[IO] = saProvider
  //behaviour defined in test\...\reference.conf
  val projectPermissions: Map[ProjectAction, Boolean] =
    (ProjectAction.allActions map (action => action -> authConfig.getBoolean(action.toString))).toMap
  val runtimePermissions: Map[RuntimeAction, Boolean] =
    (RuntimeAction.allActions map (action => action -> authConfig.getBoolean(action.toString))).toMap
  val diskPermissions: Map[PersistentDiskAction, Boolean] =
    (PersistentDiskAction.allActions map (action => action -> authConfig.getBoolean(action.toString))).toMap

  val canSeeResourcesInAllProjects = authConfig.as[Option[Boolean]]("canSeeResourcesInAllProjects").getOrElse(false)
  val canSeeAllResourcesIn = authConfig.as[Option[Seq[String]]]("canSeeAllResourcesIn").getOrElse(Seq.empty)

  override def hasProjectPermission(
    userInfo: UserInfo,
    action: ProjectAction,
    googleProject: GoogleProject
  )(implicit ev: ApplicativeAsk[IO, TraceId]): IO[Boolean] =
    IO.pure(projectPermissions(action))

  override def hasRuntimePermission(
    samResource: RuntimeSamResource,
    userInfo: UserInfo,
    action: RuntimeAction,
    googleProject: GoogleProject
  )(implicit ev: ApplicativeAsk[IO, TraceId]): IO[Boolean] =
    IO.pure(runtimePermissions(action))

  override def hasPersistentDiskPermission(
    samResource: PersistentDiskSamResource,
    userInfo: UserInfo,
    action: PersistentDiskAction,
    googleProject: GoogleProject
  )(implicit ev: ApplicativeAsk[IO, TraceId]): IO[Boolean] =
    IO.pure(diskPermissions(action))

  override def filterUserVisibleRuntimes(userInfo: UserInfo, runtimes: List[(GoogleProject, RuntimeSamResource)])(
    implicit ev: ApplicativeAsk[IO, TraceId]
  ): IO[List[(GoogleProject, RuntimeSamResource)]] =
    if (canSeeResourcesInAllProjects) {
      IO.pure(runtimes)
    } else {
      IO.pure(runtimes.filter {
        case (googleProject, _) =>
          canSeeAllResourcesIn.contains(googleProject.value) || runtimePermissions(
            RuntimeAction.GetRuntimeStatus
          )
      })
    }

  override def filterUserVisiblePersistentDisks(userInfo: UserInfo,
                                                disks: List[(GoogleProject, PersistentDiskSamResource)])(
    implicit ev: ApplicativeAsk[IO, TraceId]
  ): IO[List[(GoogleProject, PersistentDiskSamResource)]] =
    if (canSeeResourcesInAllProjects) {
      IO.pure(disks)
    } else {
      IO.pure(disks.filter {
        case (googleProject, _) =>
          canSeeAllResourcesIn.contains(googleProject.value) || diskPermissions(PersistentDiskAction.ReadPersistentDisk)
      })
    }

  private def notifyInternal: IO[Unit] =
    if (notifySucceeds)
      IO.unit
    else
      IO.raiseError(new RuntimeException("boom"))

  override def notifyResourceCreated(samResource: SamResource,
                                     creatorEmail: WorkbenchEmail,
                                     googleProject: GoogleProject)(implicit ev: ApplicativeAsk[IO, TraceId]): IO[Unit] =
    notifyInternal

  override def notifyResourceDeleted(samResource: SamResource,
                                     userEmail: WorkbenchEmail,
                                     creatorEmail: WorkbenchEmail,
                                     googleProject: GoogleProject)(implicit ev: ApplicativeAsk[IO, TraceId]): IO[Unit] =
    notifyInternal

}
