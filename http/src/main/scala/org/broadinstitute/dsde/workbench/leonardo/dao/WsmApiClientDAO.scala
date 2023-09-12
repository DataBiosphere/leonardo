package org.broadinstitute.dsde.workbench.leonardo.dao

import bio.terra.workspace.api.ControlledAzureResourceApi
import bio.terra.workspace.model.State
import cats.mtl.Ask
import org.broadinstitute.dsde.workbench.leonardo.{AppContext, AppId, WorkspaceId, WsmControlledResourceId}
import org.broadinstitute.dsde.workbench.leonardo.db.{controlledResourceQuery, WsmResourceType}

trait WsmApiClientDAO[F[_]] {

  // Returns the status of this resource in WSM, or None if it doesn't exist
  def getWsmResourceStatus(wsmResourceId: WsmControlledResourceId,
                           resourceType: WsmResourceType,
                           workspaceId: WorkspaceId
  )(implicit
    ev: Ask[F, AppContext]
  ): F[Option[State]]

  // Checks if the runtime status is deletable + sub-resources (disk, if necessary) in WSM
  def isRuntimeDeletable(runtimeId: Long,
                         wsmResourceId: WsmControlledResourceId,
                         workspaceId: WorkspaceId,
                         deleteDisk: Boolean
  )(implicit
    ev: Ask[F, AppContext]
  ): F[Boolean]

  // Checks if the app status is deletable + sub-resources (managedIdentity, database) in WSM
  def isAppDeletable(appId: AppId, wsmResourceId: WsmControlledResourceId, workspaceId: WorkspaceId)(implicit
    ev: Ask[F, AppContext]
  ): F[Boolean]

  // Sends a delete call to WSM for the specified resource
  def deleteWsmResource(wsmResourceId: WsmControlledResourceId,
                        resourceType: WsmResourceType,
                        workspaceId: WorkspaceId
  )(implicit
    ev: Ask[F, AppContext]
  ): F[Unit]

  // Deletes and polls the sub-resources (disk, storageContainer), then the VM if that succeeds
  def deleteWsmVm(runtimeId: Long, wsmResourceId: WsmControlledResourceId, workspaceId: WorkspaceId, deleteDisk: Boolean)(implicit
    ev: Ask[F, AppContext]
  ): F[Unit]

  // Deletes and polls the app sub-resources (managedIdentity, database)
  def deleteWsmAppResources(appId: AppId, wsmResourceId: WsmControlledResourceId, workspaceId: WorkspaceId)(implicit
    ev: Ask[F, AppContext]
  ): F[Unit]

}
