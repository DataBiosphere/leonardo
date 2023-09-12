package org.broadinstitute.dsde.workbench.leonardo.dao

import bio.terra.workspace.api.ControlledAzureResourceApi
import bio.terra.workspace.model.State
import cats.effect.Async
import cats.mtl.Ask
import org.broadinstitute.dsde.workbench.leonardo.{AppContext, WorkspaceId, WsmControlledResourceId}
import cats.syntax.all._
import org.broadinstitute.dsde.workbench.leonardo.db.WsmResourceType

// just abstract for the purposes of this draft
abstract class HttpWsmApiClientDAO[F[_]](wsmApi: ControlledAzureResourceApi)(implicit
  // logger: StructuredLogger[F],
  F: Async[F]
  // metrics: OpenTelemetryMetrics[F]
) extends WsmApiClientDAO[F] {

  val deletableWsmStatuses = List(State.READY, State.BROKEN)

  override def getWsmResourceStatus(wsmResourceId: WsmControlledResourceId,
                                    resourceType: WsmResourceType,
                                    workspaceId: WorkspaceId
  )(implicit
    ev: Ask[F, AppContext]
  ): F[State] =
    resourceType match {
      case WsmResourceType.AzureDisk =>
        F.delay(wsmApi.getAzureDisk(workspaceId.value, wsmResourceId.value)).map(_.getMetadata.getState)
      case WsmResourceType.AzureDatabase =>
        F.delay(wsmApi.getAzureDatabase(workspaceId.value, wsmResourceId.value)).map(_.getMetadata.getState)
      case WsmResourceType.AzureManagedIdentity =>
        F.delay(wsmApi.getAzureManagedIdentity(workspaceId.value, wsmResourceId.value)).map(_.getMetadata.getState)
      case WsmResourceType.AzureVm =>
        F.delay(wsmApi.getAzureVm(workspaceId.value, wsmResourceId.value)).map(_.getMetadata.getState)
      // TODO: add check for AzureStorageContainer once added to WsmClient
    }

}
