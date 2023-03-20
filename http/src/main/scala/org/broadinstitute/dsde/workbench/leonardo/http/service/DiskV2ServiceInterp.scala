package org.broadinstitute.dsde.workbench.leonardo
package http
package service

import akka.http.scaladsl.model.StatusCodes
import cats.Parallel
import cats.effect.Async
import cats.effect.std.Queue
import cats.mtl.Ask
import cats.syntax.all._
import org.broadinstitute.dsde.workbench.leonardo.SamResourceId.{
  WorkspaceResourceSamResourceId,
  WsmResourceSamResourceId
}
import org.broadinstitute.dsde.workbench.leonardo.config.PersistentDiskConfig
import org.broadinstitute.dsde.workbench.leonardo.dao._
import org.broadinstitute.dsde.workbench.leonardo.db._
import org.broadinstitute.dsde.workbench.leonardo.model._
import org.broadinstitute.dsde.workbench.leonardo.monitor.LeoPubsubMessage
import org.broadinstitute.dsde.workbench.leonardo.monitor.LeoPubsubMessage.DeleteAzureDiskMessage
import org.broadinstitute.dsde.workbench.model.{TraceId, UserInfo}

import java.util.UUID
import scala.concurrent.ExecutionContext

class DiskV2ServiceInterp[F[_]: Parallel](config: PersistentDiskConfig,
                                          authProvider: LeoAuthProvider[F],
                                          wsmDao: WsmDao[F],
                                          samDAO: SamDAO[F],
                                          publisherQueue: Queue[F, LeoPubsubMessage]
)(implicit
  F: Async[F],
  dbReference: DbReference[F],
  ec: ExecutionContext
) extends DiskV2Service[F] {

  private def checkSamPermission(wsmResourceSamResourceId: WsmResourceSamResourceId,
                                 userInfo: UserInfo,
                                 wsmResourceAction: WsmResourceAction
  )(implicit
    ctx: Ask[F, AppContext]
  ): F[(Boolean, WsmControlledResourceId)] =
    for {
      // TODO: generalize for google
      res <- authProvider.hasPermission(
        wsmResourceSamResourceId,
        wsmResourceAction,
        userInfo
      )
    } yield (res, wsmResourceSamResourceId.controlledResourceId)

  override def getDisk(userInfo: UserInfo, workspaceId: WorkspaceId, diskName: DiskName)(implicit
    as: Ask[F, AppContext]
  ): F[GetPersistentDiskResponse] =
    for {
      ctx <- as.ask
      diskResp <- DiskServiceDbQueries
        .getGetPersistentDiskResponse(diskName, ctx.traceId, workspaceIdOpt = Some(workspaceId))
        .transaction

      // If user is creator of the runtime, they should definitely be able to see the runtime.
      hasPermission <-
        if (diskResp.auditInfo.creator == userInfo.userEmail) F.pure(true)
        else {
          checkSamPermission(
            WsmResourceSamResourceId(WsmControlledResourceId(UUID.fromString(diskResp.samResource.resourceId))),
            userInfo,
            WsmResourceAction.Read
          ).map(_._1)
        }

      _ <- ctx.span.traverse(s => F.delay(s.addAnnotation("Done auth call for get azure disk permission")))
      _ <- F
        .raiseError[Unit](
          DiskNotFoundException(diskName, ctx.traceId, workspaceIdOpt = Some(workspaceId))
        )
        .whenA(!hasPermission)

    } yield diskResp

  override def deleteDisk(userInfo: UserInfo, workspaceId: WorkspaceId, diskName: DiskName)(implicit
    as: Ask[F, AppContext]
  ): F[Unit] =
    for {
      ctx <- as.ask
      diskOpt <- persistentDiskQuery.getActiveByNameWorkspace(workspaceId, diskName).transaction

      disk <- diskOpt.fold(
        F.raiseError[PersistentDisk](DiskNotFoundException(diskName, ctx.traceId, workspaceIdOpt = Some(workspaceId)))
      )(
        F.pure
      )
      _ <- F
        .raiseUnless(disk.status.isDeletable)(
          AzureDiskCannotBeDeletedException(workspaceId, diskName, disk.status, ctx.traceId)
        )
      hasPermission <-
        if (disk.auditInfo.creator == userInfo.userEmail)
          F.pure(true)
        else
          authProvider
            .isUserWorkspaceOwner(WorkspaceResourceSamResourceId(workspaceId), userInfo)

      _ <- ctx.span.traverse(s => F.delay(s.addAnnotation("Done auth call for delete azure runtime permission")))
      _ <- F
        .raiseError[Unit](DiskNotFoundException(diskName, ctx.traceId, workspaceIdOpt = Some(workspaceId)))
        .whenA(!hasPermission)

      _ <- persistentDiskQuery.markPendingDeletion(disk.id, ctx.now).transaction

      _ <- publisherQueue.offer(
        DeleteAzureDiskMessage(
          disk.id,
          workspaceId,
          WsmControlledResourceId(UUID.fromString(disk.samResource.resourceId)),
          Some(ctx.traceId)
        )
      )
    } yield ()
}

case class AzureDiskCannotBeDeletedException(workspaceId: WorkspaceId,
                                             diskName: DiskName,
                                             status: DiskStatus,
                                             traceId: TraceId
) extends LeoException(
      s"Persistent disk ${diskName} in workspace ${workspaceId} cannot be deleted in ${status} status",
      StatusCodes.Conflict,
      traceId = Some(traceId)
    )
