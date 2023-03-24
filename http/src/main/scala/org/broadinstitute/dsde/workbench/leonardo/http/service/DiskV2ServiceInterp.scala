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
  PersistentDiskSamResourceId,
  WorkspaceResourceSamResourceId
}
import org.broadinstitute.dsde.workbench.leonardo.config.PersistentDiskConfig
import org.broadinstitute.dsde.workbench.leonardo.dao._
import org.broadinstitute.dsde.workbench.leonardo.db._
import org.broadinstitute.dsde.workbench.leonardo.model._
import org.broadinstitute.dsde.workbench.leonardo.monitor.LeoPubsubMessage
import org.broadinstitute.dsde.workbench.leonardo.monitor.LeoPubsubMessage.DeleteDiskV2Message
import org.broadinstitute.dsde.workbench.leonardo.monitor.PubsubHandleMessageError.DiskDeletionError
import org.broadinstitute.dsde.workbench.model.{TraceId, UserInfo}

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

  override def getDisk(userInfo: UserInfo, workspaceId: WorkspaceId, diskId: DiskId)(implicit
    as: Ask[F, AppContext]
  ): F[GetPersistentDiskResponse] =
    for {
      ctx <- as.ask
      diskResp <- DiskServiceDbQueries
        .getGetPersistentDiskResponseV2(diskId, ctx.traceId, workspaceId = workspaceId)
        .transaction

      // If user is creator of the disk, they should definitely be able to see the disk.
      hasPermission <-
        if (diskResp.auditInfo.creator == userInfo.userEmail) F.pure(true)
        else {
          authProvider.hasPermission[PersistentDiskSamResourceId, PersistentDiskAction](
            diskResp.samResource,
            PersistentDiskAction.ReadPersistentDisk,
            userInfo
          )
        }

      _ <- ctx.span.traverse(s => F.delay(s.addAnnotation("Done auth call for get azure disk permission")))
      _ <- F
        .raiseError[Unit](
          DiskNotFoundByIdWorkspaceException(diskId, workspaceId, ctx.traceId)
        )
        .whenA(!hasPermission)

    } yield diskResp

  override def deleteDisk(userInfo: UserInfo, workspaceId: WorkspaceId, diskId: DiskId)(implicit
    as: Ask[F, AppContext]
  ): F[Unit] =
    for {
      ctx <- as.ask
      diskOpt <- persistentDiskQuery.getActiveByIdWorkspace(workspaceId, diskId).transaction

      disk <- diskOpt.fold(
        F.raiseError[PersistentDisk](DiskNotFoundByIdWorkspaceException(diskId, workspaceId, ctx.traceId))
      )(
        F.pure
      )
      wsmResourceId <- F.fromOption(
        disk.wsmResourceId,
        DiskDeletionError(
          disk.id,
          workspaceId,
          s"No associated resourceId found for Disk id:${disk.id.value}"
        )
      )
      _ <- F
        .raiseUnless(disk.status.isDeletable)(
          DiskCannotBeDeletedException(diskId, disk.status, disk.cloudContext, ctx.traceId)
        )
      hasPermission <-
        if (disk.auditInfo.creator == userInfo.userEmail)
          F.pure(true)
        else
          authProvider
            .isUserWorkspaceOwner(WorkspaceResourceSamResourceId(workspaceId), userInfo)

      _ <- ctx.span.traverse(s => F.delay(s.addAnnotation("Done auth call for delete azure disk permission")))
      _ <- F
        .raiseError[Unit](DiskNotFoundByIdWorkspaceException(diskId, workspaceId, ctx.traceId))
        .whenA(!hasPermission)

      // check that runtime disk was attached to is deleted
      previousRuntime <- clusterQuery.getLastClusterWithDiskId(diskId).transaction
      _ <- previousRuntime.traverse(runtime =>
        F.raiseError[Unit](DiskCannotBeDeletedAttachedException(diskId, workspaceId, ctx.traceId))
          .whenA(runtime.status == RuntimeStatus.Deleted)
      )

      _ <- persistentDiskQuery.markPendingDeletion(disk.id, ctx.now).transaction

      _ <- publisherQueue.offer(
        DeleteDiskV2Message(
          disk.id,
          workspaceId,
          disk.cloudContext,
          wsmResourceId,
          Some(ctx.traceId)
        )
      )
    } yield ()
}

case class DiskNotFoundByIdWorkspaceException(diskId: DiskId, workspaceId: WorkspaceId, traceId: TraceId)
    extends LeoException(s"Persistent disk ${diskId.value} not found in workspace ${workspaceId.value}",
                         StatusCodes.NotFound,
                         traceId = Some(traceId)
    )

case class DiskCannotBeDeletedAttachedException(id: DiskId, workspaceId: WorkspaceId, traceId: TraceId)
    extends LeoException(
      s"Persistent disk ${id.value} in workspace ${workspaceId.value} cannot be deleted. Disk is still attached to a runtime",
      StatusCodes.Conflict,
      traceId = Some(traceId)
    )
