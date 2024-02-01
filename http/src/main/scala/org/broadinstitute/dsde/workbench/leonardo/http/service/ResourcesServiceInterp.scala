package org.broadinstitute.dsde.workbench.leonardo.http.service

import cats.Parallel
import cats.effect.Async
import cats.implicits.toFunctorOps
import cats.mtl.Ask
import cats.syntax.all._
import org.broadinstitute.dsde.workbench.google2.DiskName
import org.broadinstitute.dsde.workbench.leonardo.{
  AppAction,
  AppContext,
  AppStatus,
  CloudContext,
  DiskId,
  DiskStatus,
  PersistentDiskAction,
  RuntimeAction,
  RuntimeConfig
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
  ListAppResponse,
  ListPersistentDiskResponse,
  ListRuntimeResponse2
}
import org.broadinstitute.dsde.workbench.leonardo.model.{
  ForbiddenError,
  LeoAuthProvider,
  NonDeletableDisksInProjectFoundException,
  NonDeletableRuntimesInProjectFoundException,
  RuntimeNotFoundException
}
import org.broadinstitute.dsde.workbench.model.google.GoogleProject
import org.broadinstitute.dsde.workbench.model.UserInfo

import scala.concurrent.ExecutionContext

final class ResourcesServiceInterp[F[_]: Parallel](authProvider: LeoAuthProvider[F],
                                                   runtimeService: RuntimeService[F],
                                                   appService: AppService[F],
                                                   diskService: DiskService[F]
)(implicit
  F: Async[F],
  dbRef: DbReference[F],
  ec: ExecutionContext
) extends ResourcesService[F] {
  override def deleteAllResources(userInfo: UserInfo,
                                  googleProject: GoogleProject,
                                  deleteInCloud: Boolean,
                                  deleteDisk: Boolean
  )(implicit
    as: Ask[F, AppContext]
  ): F[Unit] =
    for {
      ctx <- as.ask
      // throw 403 if no project-level permission, fail fast if the user does not have access to the project
      hasProjectPermission <- authProvider.isUserProjectReader(
        CloudContext.Gcp(googleProject),
        userInfo
      )
      _ <- F.raiseWhen(!hasProjectPermission)(ForbiddenError(userInfo.userEmail, Some(ctx.traceId)))
      _ <-
        if (deleteInCloud) deleteAllResourcesInCloud(userInfo, googleProject, deleteDisk)
        else deleteAllResourcesRecords(userInfo, googleProject)
    } yield ()

  private def deleteAllResourcesInCloud(userInfo: UserInfo, googleProject: GoogleProject, deleteDisk: Boolean)(implicit
    as: Ask[F, AppContext]
  ): F[Unit] =
    for {
      // Delete runtimes and apps, and their attached disks if deleteDisk flag is set to true
      runtimeDiskIds <- deleteAllRuntimesInCloud(userInfo, googleProject, deleteDisk)
      appDiskNames <- deleteAllAppsInCloud(userInfo, googleProject, deleteDisk)
      // Delete any potential left over orphaned disk in the project
      _ <-
        if (deleteDisk) deleteAllOrphanedDisksInCloud(userInfo, googleProject, runtimeDiskIds, appDiskNames) else F.unit
    } yield ()

  private def deleteAllRuntimesInCloud(userInfo: UserInfo, googleProject: GoogleProject, deleteDisk: Boolean)(implicit
    as: Ask[F, AppContext]
  ): F[Option[Vector[DiskId]]] =
    for {
      ctx <- as.ask
      runtimes <- runtimeService.listRuntimes(userInfo, Some(CloudContext.Gcp(googleProject)), Map.empty)

      attachedPersistentDiskIds = runtimes
        .map(r => r.runtimeConfig)
        .traverse(c =>
          c match {
            case x: RuntimeConfig.GceWithPdConfig => x.persistentDiskId
            case _                                => none
          }
        )

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
          runtimeService.deleteRuntime(DeleteRuntimeRequest(userInfo, googleProject, runtime.clusterName, deleteDisk))
        )
    } yield attachedPersistentDiskIds

  private def deleteAllAppsInCloud(userInfo: UserInfo, googleProject: GoogleProject, deleteDisk: Boolean)(implicit
    as: Ask[F, AppContext]
  ): F[Vector[Option[DiskName]]] =
    for {
      ctx <- as.ask
      apps <- appService.listApp(
        userInfo,
        Some(CloudContext.Gcp(googleProject)),
        Map.empty
      )
      attachedPersistentDiskNames = apps.map(a => a.diskName)

      nonDeletableApps = apps.filterNot(app => AppStatus.deletableStatuses.contains(app.status))

      _ <- F
        .raiseError(DeleteAllAppsV1CannotBePerformed(googleProject, nonDeletableApps, ctx.traceId))
        .whenA(!nonDeletableApps.isEmpty)

      _ <- apps
        .traverse { app =>
          appService.deleteApp(userInfo, CloudContext.Gcp(googleProject), app.appName, deleteDisk)
        }
    } yield attachedPersistentDiskNames

  private def deleteAllOrphanedDisksInCloud(userInfo: UserInfo,
                                            googleProject: GoogleProject,
                                            runtimeDiskIds: Option[Vector[DiskId]],
                                            appDisksNames: Vector[Option[DiskName]]
  )(implicit
    as: Ask[F, AppContext]
  ): F[Unit] =
    for {
      ctx <- as.ask
      // Get a list of disks that are not attached to any runtime or app
      disks <- diskService.listDisks(
        userInfo,
        Some(CloudContext.Gcp(googleProject)),
        Map.empty
      )
      orphanedDisks = disks.filterNot(disk => runtimeDiskIds.contains(disk.id) | appDisksNames.contains(disk.name))
      nonDeletableDisks = orphanedDisks.filterNot(disk => DiskStatus.deletableStatuses.contains(disk.status))

      _ <- F
        .raiseError(NonDeletableDisksInProjectFoundException(googleProject, nonDeletableDisks, ctx.traceId))
        .whenA(!nonDeletableDisks.isEmpty)

      _ <- orphanedDisks
        .traverse { disk =>
          diskService.deleteDisk(userInfo, googleProject, disk.name)
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
      runtimes <- runtimeService.listRuntimes(userInfo, Some(CloudContext.Gcp(googleProject)), Map.empty)
      _ <- runtimes.traverse(runtime => deleteRuntimeRecords(userInfo, googleProject, runtime))
    } yield ()

  private def deleteRuntimeRecords(userInfo: UserInfo, googleProject: GoogleProject, runtime: ListRuntimeResponse2)(
    implicit as: Ask[F, AppContext]
  ): F[Unit] =
    for {
      ctx <- as.ask

      listOfPermissions <- authProvider.getActions(runtime.samResource, userInfo)
      // throw 404 if no GetRuntime permission
      hasPermission = listOfPermissions.toSet.contains(RuntimeAction.GetRuntimeStatus)
      _ <-
        if (hasPermission) F.unit
        else
          F.raiseError[Unit](
            RuntimeNotFoundException(CloudContext.Gcp(googleProject),
                                     runtime.clusterName,
                                     "Permission Denied",
                                     Some(ctx.traceId)
            )
          )

      // throw 403 if no DeleteApp permission
      hasDeletePermission = listOfPermissions.toSet.contains(RuntimeAction.DeleteRuntime)
      _ <- if (hasDeletePermission) F.unit else F.raiseError[Unit](ForbiddenError(userInfo.userEmail))

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
      apps <- appService.listApp(
        userInfo,
        Some(CloudContext.Gcp(googleProject)),
        Map.empty
      )
      _ <- apps.traverse(app => deleteAppRecords(userInfo, googleProject, app))
    } yield ()

  private def deleteAppRecords(userInfo: UserInfo, googleProject: GoogleProject, app: ListAppResponse)(implicit
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
      listOfPermissions <- authProvider.getActions(dbApp.app.samResourceId, userInfo)

      // throw 404 if no GetAppStatus permission
      hasPermission = listOfPermissions.toSet.contains(AppAction.GetAppStatus)
      _ <-
        if (hasPermission) F.unit
        else
          F.raiseError[Unit](
            AppNotFoundException(CloudContext.Gcp(googleProject), app.appName, ctx.traceId, "Permission Denied")
          )

      // throw 403 if no DeleteApp permission
      hasDeletePermission = listOfPermissions.toSet.contains(AppAction.DeleteApp)
      _ <- if (hasDeletePermission) F.unit else F.raiseError[Unit](ForbiddenError(userInfo.userEmail))

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
      disks <- diskService.listDisks(
        userInfo,
        Some(CloudContext.Gcp(googleProject)),
        Map.empty
      )
      _ <- disks.traverse(disk => deletediskRecords(userInfo, googleProject, disk))
    } yield ()

  private def deletediskRecords(userInfo: UserInfo, googleProject: GoogleProject, disk: ListPersistentDiskResponse)(
    implicit as: Ask[F, AppContext]
  ): F[Unit] =
    for {
      ctx <- as.ask
      // Find the disk's Sam resource id
      dbdisk <- DiskServiceDbQueries
        .getGetPersistentDiskResponse(CloudContext.Gcp(googleProject), disk.name, ctx.traceId)
        .transaction

      listOfPermissions <- authProvider.getActions(dbdisk.samResource, userInfo)

      // throw 404 if no ReadDiskStatus permission
      hasPermission = listOfPermissions.toSet.contains(PersistentDiskAction.ReadPersistentDisk)
      _ <-
        if (hasPermission) F.unit
        else
          F.raiseError[Unit](
            DiskNotFoundException(CloudContext.Gcp(googleProject), disk.name, ctx.traceId)
          )

      // throw 403 if no DeleteDisk permission
      hasDeletePermission = listOfPermissions.toSet.contains(PersistentDiskAction.DeletePersistentDisk)
      _ <- if (hasDeletePermission) F.unit else F.raiseError[Unit](ForbiddenError(userInfo.userEmail))

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
}
