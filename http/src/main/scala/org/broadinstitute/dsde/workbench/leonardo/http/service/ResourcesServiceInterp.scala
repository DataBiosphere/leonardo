package org.broadinstitute.dsde.workbench.leonardo.http.service

import cats.Parallel
import cats.effect.Async
import cats.implicits.toFunctorOps
import cats.mtl.Ask
import cats.syntax.all._
import org.broadinstitute.dsde.workbench.leonardo.{AppContext, CloudContext}
import org.broadinstitute.dsde.workbench.leonardo.http.ctxConversion
import org.broadinstitute.dsde.workbench.leonardo.model.{ForbiddenError, LeoAuthProvider}
import org.broadinstitute.dsde.workbench.model.UserInfo

final class ResourcesServiceInterp[F[_]: Parallel](authProvider: LeoAuthProvider[F],
                                                   runtimeService: RuntimeService[F],
                                                   appService: AppService[F],
                                                   diskService: DiskService[F]
)(implicit
  F: Async[F]
) extends ResourcesService[F] {

  override def deleteAllResourcesInCloud(userInfo: UserInfo, cloudContext: CloudContext.Gcp, deleteDisk: Boolean)(
    implicit as: Ask[F, AppContext]
  ): F[Unit] =
    for {
      ctx <- as.ask
      // throw 403 if no project-level permission, fail fast if the user does not have access to the project
      hasProjectPermission <- authProvider.isUserProjectReader(
        cloudContext,
        userInfo
      )
      _ <- F.raiseWhen(!hasProjectPermission)(ForbiddenError(userInfo.userEmail, Some(ctx.traceId)))

      // Delete runtimes and apps, and their attached disks if deleteDisk flag is set to true
      runtimeDiskIdsOpt <- runtimeService.deleteAllRuntimes(userInfo, cloudContext, deleteDisk)
      runtimeDiskIds = runtimeDiskIdsOpt.getOrElse(Vector.empty)
      appDiskNames <- appService.deleteAllApps(userInfo, cloudContext, deleteDisk)
      // Delete any potential left over orphaned disk in the project
      _ <-
        if (deleteDisk) diskService.deleteAllOrphanedDisks(userInfo, cloudContext, runtimeDiskIds, appDiskNames)
        else F.unit
    } yield ()

  override def deleteAllResourcesRecords(userInfo: UserInfo, cloudContext: CloudContext.Gcp)(implicit
    as: Ask[F, AppContext]
  ): F[Unit] =
    for {
      // Note that we are intentionally not checking for project-level permission here as this funtion is intended to be called
      // AFTER the google project has been deleted
      _ <- runtimeService.deleteAllRuntimesRecords(userInfo, cloudContext)
      _ <- appService.deleteAllAppsRecords(userInfo, cloudContext)
      _ <- diskService.deleteAllDisksRecords(userInfo, cloudContext)
    } yield ()

}
