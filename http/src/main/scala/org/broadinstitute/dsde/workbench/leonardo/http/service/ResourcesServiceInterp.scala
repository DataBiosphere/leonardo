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
      cloudContext = CloudContext.Gcp(googleProject)
      // throw 403 if no project-level permission, fail fast if the user does not have access to the project
      hasProjectPermission <- authProvider.isUserProjectReader(
        cloudContext,
        userInfo
      )
      _ <- F.raiseWhen(!hasProjectPermission)(ForbiddenError(userInfo.userEmail, Some(ctx.traceId)))

      _ <-
        if (deleteInCloud) deleteAllResourcesInCloud(userInfo, cloudContext, deleteDisk)
        else deleteAllResourcesRecords(userInfo, cloudContext)
    } yield ()

  private def deleteAllResourcesInCloud(userInfo: UserInfo, cloudContext: CloudContext.Gcp, deleteDisk: Boolean)(
    implicit as: Ask[F, AppContext]
  ): F[Unit] =
    for {
      // Delete runtimes and apps, and their attached disks if deleteDisk flag is set to true
      runtimeDiskIds <- runtimeService.deleteAllRuntimes(userInfo, cloudContext, deleteDisk)
      appDiskNames <- appService.deleteAllApps(userInfo, cloudContext, deleteDisk)
      // Delete any potential left over orphaned disk in the project
      _ <-
        if (deleteDisk) diskService.deleteAllOrphanedDisks(userInfo, cloudContext, runtimeDiskIds, appDiskNames)
        else F.unit
    } yield ()

  private def deleteAllResourcesRecords(userInfo: UserInfo, cloudContext: CloudContext.Gcp)(implicit
    as: Ask[F, AppContext]
  ): F[Unit] =
    for {
      _ <- runtimeService.deleteAllRuntimesRecords(userInfo, cloudContext)
      _ <- appService.deleteAllAppsRecords(userInfo, cloudContext)
      _ <- diskService.deleteAllDisksRecords(userInfo, cloudContext)
    } yield ()

}
