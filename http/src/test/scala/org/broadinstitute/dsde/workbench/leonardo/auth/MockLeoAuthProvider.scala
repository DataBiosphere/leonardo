package org.broadinstitute.dsde.workbench.leonardo
package auth

import cats.effect.IO
import cats.mtl.ApplicativeAsk
import com.typesafe.config.Config
import net.ceedubs.ficus.Ficus._
import org.broadinstitute.dsde.workbench.leonardo.model._
import org.broadinstitute.dsde.workbench.model.google.GoogleProject
import org.broadinstitute.dsde.workbench.model.{TraceId, UserInfo, WorkbenchEmail}

class MockLeoAuthProvider(authConfig: Config, saProvider: ServiceAccountProvider[IO], notifySucceeds: Boolean = true)
    extends LeoAuthProvider[IO] {
  override def serviceAccountProvider: ServiceAccountProvider[IO] = saProvider
  //behaviour defined in test\...\reference.conf
  val projectPermissions: Map[ProjectAction, Boolean] =
    (ProjectAction.allActions map (action => action -> authConfig.getBoolean(action.toString))).toMap
  val clusterPermissions: Map[NotebookClusterAction, Boolean] =
    (NotebookClusterAction.allActions map (action => action -> authConfig.getBoolean(action.toString))).toMap
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

  override def hasNotebookClusterPermission(
    internalId: RuntimeInternalId,
    userInfo: UserInfo,
    action: NotebookClusterAction,
    googleProject: GoogleProject,
    clusterName: RuntimeName
  )(implicit ev: ApplicativeAsk[IO, TraceId]): IO[Boolean] =
    IO.pure(clusterPermissions(action))

  override def hasPersistentDiskPermission(
    internalId: DiskSamResourceId,
    userInfo: UserInfo,
    action: PersistentDiskAction,
    googleProject: GoogleProject
  )(implicit ev: ApplicativeAsk[IO, TraceId]): IO[Boolean] =
    IO.pure(diskPermissions(action))

  override def filterUserVisibleClusters(userInfo: UserInfo, clusters: List[(GoogleProject, RuntimeInternalId)])(
    implicit ev: ApplicativeAsk[IO, TraceId]
  ): IO[List[(GoogleProject, RuntimeInternalId)]] =
    if (canSeeResourcesInAllProjects) {
      IO.pure(clusters)
    } else {
      IO.pure(clusters.filter {
        case (googleProject, _) =>
          canSeeAllResourcesIn.contains(googleProject.value) || clusterPermissions(
            NotebookClusterAction.GetClusterStatus
          )
      })
    }

  override def filterUserVisiblePersistentDisks(userInfo: UserInfo, disks: List[(GoogleProject, DiskSamResourceId)])(
    implicit ev: ApplicativeAsk[IO, TraceId]
  ): IO[List[(GoogleProject, DiskSamResourceId)]] =
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

  override def notifyClusterCreated(internalId: RuntimeInternalId,
                                    creatorEmail: WorkbenchEmail,
                                    googleProject: GoogleProject,
                                    clusterName: RuntimeName)(implicit ev: ApplicativeAsk[IO, TraceId]): IO[Unit] =
    notifyInternal

  override def notifyClusterDeleted(internalId: RuntimeInternalId,
                                    userEmail: WorkbenchEmail,
                                    creatorEmail: WorkbenchEmail,
                                    googleProject: GoogleProject,
                                    clusterName: RuntimeName)(implicit ev: ApplicativeAsk[IO, TraceId]): IO[Unit] =
    notifyInternal

  override def notifyPersistentDiskCreated(
    internalId: DiskSamResourceId,
    creatorEmail: WorkbenchEmail,
    googleProject: GoogleProject
  )(implicit ev: ApplicativeAsk[IO, TraceId]): IO[Unit] =
    notifyInternal

  override def notifyPersistentDiskDeleted(
    internalId: DiskSamResourceId,
    userEmail: WorkbenchEmail,
    creatorEmail: WorkbenchEmail,
    googleProject: GoogleProject
  )(implicit ev: ApplicativeAsk[IO, TraceId]): IO[Unit] =
    notifyInternal

}
