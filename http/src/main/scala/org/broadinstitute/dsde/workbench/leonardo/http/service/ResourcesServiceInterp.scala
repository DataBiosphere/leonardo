package org.broadinstitute.dsde.workbench.leonardo.http.service

import cats.Parallel
import cats.effect.Async
import cats.implicits.toFunctorOps
import cats.mtl.Ask
import cats.syntax.all._
import org.broadinstitute.dsde.workbench.google2.DiskName
import org.broadinstitute.dsde.workbench.leonardo.{
  AppContext,
  AppName,
  AppStatus,
  CloudContext,
  DiskStatus,
  RuntimeName,
  WorkspaceId
}
import org.broadinstitute.dsde.workbench.leonardo.db.{
  appQuery,
  clusterQuery,
  kubernetesClusterQuery,
  nodepoolQuery,
  persistentDiskQuery,
  DbReference,
  DiskServiceDbQueries,
  KubernetesServiceDbQueries
}
import org.broadinstitute.dsde.workbench.leonardo.http.{
  ctxConversion,
  CreateAppRequest,
  CreateDiskRequest,
  CreateRuntimeRequest,
  CreateRuntimeResponse,
  GetAppResponse,
  GetPersistentDiskResponse,
  GetRuntimeResponse,
  ListAppResponse,
  ListPersistentDiskResponse,
  ListRuntimeResponse2,
  UpdateDiskRequest,
  UpdateRuntimeRequest
}
import org.broadinstitute.dsde.workbench.leonardo.model.{
  LeoAuthProvider,
  NonDeletableDisksInProjectFoundException,
  NonDeletableRuntimesInProjectFoundException
}
import org.broadinstitute.dsde.workbench.model.google.GoogleProject
import org.broadinstitute.dsde.workbench.model.UserInfo

import scala.concurrent.ExecutionContext

final class ResourcesServiceInterp[F[_]: Parallel](authProvider: LeoAuthProvider[F])(implicit
  F: Async[F],
  dbRef: DbReference[F],
  ec: ExecutionContext
) extends ResourcesService[F]
    with RuntimeService[F]
    with AppService[F]
    with DiskService[F] {
  override def deleteAllResources(userInfo: UserInfo,
                                  googleProject: GoogleProject,
                                  deleteInCloud: Boolean,
                                  deleteDisk: Boolean
  )(implicit
    as: Ask[F, AppContext]
  ): F[Unit] =
    for {
      ctx <- as.ask
      _ <-
        if (deleteInCloud) deleteAllResourcesInCloud(userInfo, googleProject, deleteDisk)
        else deleteAllResourcesRecords(userInfo, googleProject)
    } yield ()

  private def deleteAllResourcesInCloud(userInfo: UserInfo, googleProject: GoogleProject, deleteDisk: Boolean)(implicit
    as: Ask[F, AppContext]
  ): F[Unit] =
    for {
      _ <- deleteAllRuntimesInCloud(userInfo, googleProject, deleteDisk)
      _ <- deleteAllAppsInCloud(userInfo, googleProject, deleteDisk)
      // Delete any potential left over orphaned disk in the project - This may be a complete overkill as the deletion
      // of most disks done as part of delete runtimes and delete apps will still be in progress at that point
      _ <- if (deleteDisk) deleteAllDisksInCloud(userInfo, googleProject) else F.unit
    } yield ()

  private def deleteAllRuntimesInCloud(userInfo: UserInfo, googleProject: GoogleProject, deleteDisk: Boolean)(implicit
    as: Ask[F, AppContext]
  ): F[Unit] =
    for {
      ctx <- as.ask
      runtimes <- listRuntimes(userInfo, Some(CloudContext.Gcp(googleProject)), Map.empty)
      nonDeletableRuntimes = runtimes.filterNot(r => r.status.isDeletable)

      _ <- F
        .raiseError(
          NonDeletableRuntimesInProjectFoundException(
            googleProject,
            nonDeletableRuntimes,
            ctx.traceId
          )
        )
        .whenA(!nonDeletableRuntimes.isEmpty)

      _ <- runtimes
        .traverse(runtime =>
          deleteRuntime(DeleteRuntimeRequest(userInfo, googleProject, runtime.clusterName, deleteDisk))
        )
    } yield ()

  private def deleteAllAppsInCloud(userInfo: UserInfo, googleProject: GoogleProject, deleteDisk: Boolean)(implicit
    as: Ask[F, AppContext]
  ): F[Unit] =
    for {
      ctx <- as.ask
      apps <- listApp(
        userInfo,
        Some(CloudContext.Gcp(googleProject)),
        Map.empty
      )
      nonDeletableApps = apps.filterNot(app => AppStatus.deletableStatuses.contains(app.status))

      _ <- F
        .raiseError(DeleteAllAppsV1CannotBePerformed(googleProject, nonDeletableApps, ctx.traceId))
        .whenA(!nonDeletableApps.isEmpty)

      _ <- apps
        .traverse { app =>
          deleteApp(userInfo, CloudContext.Gcp(googleProject), app.appName, deleteDisk)
        }
    } yield ()

  private def deleteAllDisksInCloud(userInfo: UserInfo, googleProject: GoogleProject)(implicit
    as: Ask[F, AppContext]
  ): F[Unit] =
    for {
      ctx <- as.ask
      disks <- listDisks(
        userInfo,
        Some(CloudContext.Gcp(googleProject)),
        Map.empty
      )
      nonDeletableDisks = disks.filterNot(disk => DiskStatus.deletableStatuses.contains(disk.status))

      _ <- F
        .raiseError(NonDeletableDisksInProjectFoundException(googleProject, nonDeletableDisks, ctx.traceId))
        .whenA(!nonDeletableDisks.isEmpty)

      _ <- disks
        .traverse { disk =>
          deleteDisk(userInfo, googleProject, disk.name)
        }
    } yield ()

  private def deleteAllResourcesRecords(userInfo: UserInfo, googleProject: GoogleProject)(implicit
    as: Ask[F, AppContext]
  ): F[Unit] =
    for {
      ctx <- as.ask
      _ <- deleteAllRuntimesRecords(userInfo, googleProject)
      _ <- deleteAllAppsRecords(userInfo, googleProject)
      _ <- deleteAllDisksRecords(userInfo, googleProject)
    } yield ()

  private def deleteAllRuntimesRecords(userInfo: UserInfo, googleProject: GoogleProject)(implicit
    as: Ask[F, AppContext]
  ): F[Unit] =
    for {
      ctx <- as.ask
      runtimes <- listRuntimes(userInfo, Some(CloudContext.Gcp(googleProject)), Map.empty)
      _ <- runtimes.traverse(runtime => deleteRuntimeRecords(googleProject, runtime))
    } yield ()

  private def deleteRuntimeRecords(googleProject: GoogleProject, runtime: ListRuntimeResponse2)(implicit
    as: Ask[F, AppContext]
  ): F[Unit] =
    for {
      ctx <- as.ask
      // Mark the resource as deleted in Leo's DB
      _ <- dbRef.inTransaction(clusterQuery.completeDeletion(runtime.id, ctx.now))
      // Notify SAM that the resource has been deleted
      _ <- authProvider
        .notifyResourceDeleted(
          runtime.samResource,
          runtime.auditInfo.creator,
          googleProject
        )
    } yield ()

  private def deleteAllAppsRecords(userInfo: UserInfo, googleProject: GoogleProject)(implicit
    as: Ask[F, AppContext]
  ): F[Unit] =
    for {
      ctx <- as.ask
      apps <- listApp(
        userInfo,
        Some(CloudContext.Gcp(googleProject)),
        Map.empty
      )
      _ <- apps.traverse(app => deleteAppRecords(googleProject, app))
    } yield ()

  private def deleteAppRecords(googleProject: GoogleProject, app: ListAppResponse)(implicit
    as: Ask[F, AppContext]
  ): F[Unit] =
    for {
      ctx <- as.ask
      // Find the app ID and Sam resource id
      dbAppOpt <- KubernetesServiceDbQueries
        .getActiveFullAppByName(CloudContext.Gcp(googleProject), app.appName)
        .transaction
      dbApp <- F.fromOption(
        dbAppOpt,
        AppNotFoundException(CloudContext.Gcp(googleProject), app.appName, ctx.traceId, "No active app found in DB")
      )
      // Mark the app, nodepool and cluster as deleted in Leo's DB
      _ <- dbRef.inTransaction(appQuery.markAsDeleted(dbApp.app.id, ctx.now))
      _ <- dbRef.inTransaction(nodepoolQuery.markAsDeleted(dbApp.nodepool.id, ctx.now))
      _ <- dbRef.inTransaction(kubernetesClusterQuery.markAsDeleted(dbApp.cluster.id, ctx.now))
      // Notify SAM that the resource has been deleted
      _ <- authProvider
        .notifyResourceDeleted(
          dbApp.app.samResourceId,
          dbApp.app.auditInfo.creator,
          googleProject
        )
    } yield ()

  private def deleteAllDisksRecords(userInfo: UserInfo, googleProject: GoogleProject)(implicit
    as: Ask[F, AppContext]
  ): F[Unit] =
    for {
      ctx <- as.ask
      disks <- listDisks(
        userInfo,
        Some(CloudContext.Gcp(googleProject)),
        Map.empty
      )
      _ <- disks.traverse(disk => deletediskRecords(googleProject, disk))
    } yield ()

  private def deletediskRecords(googleProject: GoogleProject, disk: ListPersistentDiskResponse)(implicit
    as: Ask[F, AppContext]
  ): F[Unit] =
    for {
      ctx <- as.ask
      // Find the disk's Sam resource id
      dbdisk <- DiskServiceDbQueries
        .getGetPersistentDiskResponse(CloudContext.Gcp(googleProject), disk.name, ctx.traceId)
        .transaction
      // Mark the resource as deleted in Leo's DB
      _ <- dbRef.inTransaction(persistentDiskQuery.delete(disk.id, ctx.now))
      // Notify SAM that the resource has been deleted
      _ <- authProvider
        .notifyResourceDeleted(
          dbdisk.samResource,
          disk.auditInfo.creator,
          googleProject
        )
    } yield ()

  // Ugly workaround to avoid making ResourcesServiceInterp abstract
  override def createRuntime(userInfo: UserInfo,
                             cloudContext: CloudContext,
                             runtimeName: RuntimeName,
                             req: CreateRuntimeRequest
  )(implicit as: Ask[F, AppContext]): F[CreateRuntimeResponse] = ???

  override def getRuntime(userInfo: UserInfo, cloudContext: CloudContext, runtimeName: RuntimeName)(implicit
    as: Ask[F, AppContext]
  ): F[GetRuntimeResponse] = ???

  override def listRuntimes(userInfo: UserInfo, cloudContext: Option[CloudContext], params: Map[String, String])(
    implicit as: Ask[F, AppContext]
  ): F[Vector[ListRuntimeResponse2]] = ???

  override def deleteRuntime(deleteRuntimeRequest: DeleteRuntimeRequest)(implicit as: Ask[F, AppContext]): F[Unit] = ???

  override def stopRuntime(userInfo: UserInfo, cloudContext: CloudContext, runtimeName: RuntimeName)(implicit
    as: Ask[F, AppContext]
  ): F[Unit] = ???

  override def startRuntime(userInfo: UserInfo, googleProject: GoogleProject, runtimeName: RuntimeName)(implicit
    as: Ask[F, AppContext]
  ): F[Unit] = ???

  override def updateRuntime(userInfo: UserInfo,
                             googleProject: GoogleProject,
                             runtimeName: RuntimeName,
                             req: UpdateRuntimeRequest
  )(implicit as: Ask[F, AppContext]): F[Unit] = ???

  override def createApp(userInfo: UserInfo,
                         cloudContext: CloudContext.Gcp,
                         appName: AppName,
                         req: CreateAppRequest,
                         workspaceId: Option[WorkspaceId]
  )(implicit as: Ask[F, AppContext]): F[Unit] = ???

  override def getApp(userInfo: UserInfo, cloudContext: CloudContext.Gcp, appName: AppName)(implicit
    as: Ask[F, AppContext]
  ): F[GetAppResponse] = ???

  override def listApp(userInfo: UserInfo, cloudContext: Option[CloudContext.Gcp], params: Map[String, String])(implicit
    as: Ask[F, AppContext]
  ): F[Vector[ListAppResponse]] = ???

  override def deleteApp(userInfo: UserInfo, cloudContext: CloudContext.Gcp, appName: AppName, deleteDisk: Boolean)(
    implicit as: Ask[F, AppContext]
  ): F[Unit] = ???

  override def stopApp(userInfo: UserInfo, cloudContext: CloudContext.Gcp, appName: AppName)(implicit
    as: Ask[F, AppContext]
  ): F[Unit] = ???

  override def startApp(userInfo: UserInfo, cloudContext: CloudContext.Gcp, appName: AppName)(implicit
    as: Ask[F, AppContext]
  ): F[Unit] = ???

  override def createAppV2(userInfo: UserInfo, workspaceId: WorkspaceId, appName: AppName, req: CreateAppRequest)(
    implicit as: Ask[F, AppContext]
  ): F[Unit] = ???

  override def getAppV2(userInfo: UserInfo, workspaceId: WorkspaceId, appName: AppName)(implicit
    as: Ask[F, AppContext]
  ): F[GetAppResponse] = ???

  override def listAppV2(userInfo: UserInfo, workspaceId: WorkspaceId, params: Map[String, String])(implicit
    as: Ask[F, AppContext]
  ): F[Vector[ListAppResponse]] = ???

  override def deleteAppV2(userInfo: UserInfo, workspaceId: WorkspaceId, appName: AppName, deleteDisk: Boolean)(implicit
    as: Ask[F, AppContext]
  ): F[Unit] = ???

  override def deleteAllAppsV2(userInfo: UserInfo, workspaceId: WorkspaceId, deleteDisk: Boolean)(implicit
    as: Ask[F, AppContext]
  ): F[Unit] = ???

  override def createDisk(userInfo: UserInfo, googleProject: GoogleProject, diskName: DiskName, req: CreateDiskRequest)(
    implicit as: Ask[F, AppContext]
  ): F[Unit] = ???

  override def getDisk(userInfo: UserInfo, cloudContext: CloudContext, diskName: DiskName)(implicit
    as: Ask[F, AppContext]
  ): F[GetPersistentDiskResponse] = ???

  override def listDisks(userInfo: UserInfo, cloudContext: Option[CloudContext], params: Map[String, String])(implicit
    as: Ask[F, AppContext]
  ): F[Vector[ListPersistentDiskResponse]] = ???

  override def deleteDisk(userInfo: UserInfo, googleProject: GoogleProject, diskName: DiskName)(implicit
    as: Ask[F, AppContext]
  ): F[Unit] = ???

  override def updateDisk(userInfo: UserInfo, googleProject: GoogleProject, diskName: DiskName, req: UpdateDiskRequest)(
    implicit as: Ask[F, AppContext]
  ): F[Unit] = ???
}
