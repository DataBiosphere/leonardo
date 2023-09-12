package org.broadinstitute.dsde.workbench.leonardo.dao

import bio.terra.workspace.api.ControlledAzureResourceApi
import bio.terra.workspace.model.State
import cats.effect.Async
import cats.mtl.Ask
import org.broadinstitute.dsde.workbench.leonardo.{AppContext, AppId, WorkspaceId, WsmControlledResourceId}
import org.broadinstitute.dsde.workbench.openTelemetry.OpenTelemetryMetrics
import bio.terra.workspace.model.State
import org.typelevel.log4cats.StructuredLogger
import cats.syntax.all._
import org.broadinstitute.dsde.workbench.leonardo.db.WsmResourceType

// TODO: just abstract for the purposes of this draft
abstract class HttpWsmApiClientDAO[F[_]](wsmApi: ControlledAzureResourceApi)(implicit
  logger: StructuredLogger[F],
  F: Async[F],
  metrics: OpenTelemetryMetrics[F]
) extends WsmApiClientDAO[F] {

  val deletableWsmStatuses = List(State.READY, State.BROKEN)

  override def getWsmResourceStatus(wsmResourceId: WsmControlledResourceId,
                                    resourceType: WsmResourceType,
                                    workspaceId: WorkspaceId
  )(implicit
    ev: Ask[F, AppContext]
  ): F[Option[State]] =
    resourceType match {
      case WsmResourceType.AzureDisk =>
        F.delay(wsmApi.getAzureDisk(workspaceId.value, wsmResourceId.value)).map(_.getMetadata.getName)
      case WsmResourceType.AzureDatabase =>
        F.delay(wsmApi.getAzureDatabase(workspaceId.value, wsmResourceId.value)).map(_.getMetadata.getName)
      case WsmResourceType.AzureManagedIdentity =>
        F.delay(wsmApi.getAzureManagedIdentity(workspaceId.value, wsmResourceId.value)).map(_.getMetadata.getName)
      case WsmResourceType.AzureVm =>
        F.delay(wsmApi.getAzureVm(workspaceId.value, wsmResourceId.value)).map(_.getMetadata.getName)
      // TODO: add check for AzureStorageContainer once added to WsmClient
    }

  // Checks if the runtime status is deletable + sub-resources (disk, if necessary) in WSM
  override def isRuntimeDeletable(runtimeId: Long,
                                  wsmResourceId: WsmControlledResourceId,
                                  workspaceId: WorkspaceId,
                                  deleteDisk: Boolean
  )(implicit
    ev: Ask[F, AppContext]
  ): F[Boolean]

  // Checks if the app status is deletable + sub-resources (managedIdentity, database) in WSM
  override def isAppDeletable(appId: AppId, wsmResourceId: WsmControlledResourceId, workspaceId: WorkspaceId)(implicit
    ev: Ask[F, AppContext]
  ): F[Boolean]

  // Sends a delete call to WSM for the specified resource
  override def deleteWsmResource(wsmResourceId: WsmControlledResourceId,
                                 resourceType: WsmResourceType,
                                 workspaceId: WorkspaceId
  )(implicit
    ev: Ask[F, AppContext]
  ): F[Unit]

  // Deletes and polls the sub-resources (disk, storageContainer), then the VM if that succeeds
  override def deleteWsmVm(runtimeId: Long,
                           wsmResourceId: WsmControlledResourceId,
                           workspaceId: WorkspaceId,
                           deleteDisk: Boolean
  )(implicit
    ev: Ask[F, AppContext]
  ): F[Unit]

  // Deletes and polls the app sub-resources (managedIdentity, database)
  override def deleteWsmAppResources(appId: AppId, wsmResourceId: WsmControlledResourceId, workspaceId: WorkspaceId)(
    implicit ev: Ask[F, AppContext]
  ): F[Unit]

}
