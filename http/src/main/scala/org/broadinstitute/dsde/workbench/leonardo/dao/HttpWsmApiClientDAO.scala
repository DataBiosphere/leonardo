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

  override def isRuntimeDeletable(runtimeId: Long,
                                  wsmRuntimeResourceId: WsmControlledResourceId,
                                  workspaceId: WorkspaceId,
                                  wsmDiskResourceId: Option[WsmControlledResourceId]
  )(implicit
    ev: Ask[F, AppContext]
  ): F[Boolean] = for {
    runtimeStatus <- F
      .delay(wsmApi.getAzureVm(workspaceId.value, wsmRuntimeResourceId.value))
      .map(_.getMetadata.getState)
    vmDeletable = deletableWsmStatuses.contains(runtimeStatus)
    runtimeDeletable = (vmDeletable, wsmDiskResourceId) match {
      case (true, Some(wsmDiskResourceId)) =>
        for {
          diskStatus <- F
            .delay(wsmApi.getAzureDisk(workspaceId.value, wsmDiskResourceId.value))
            .map(_.getMetadata.getState)
        } yield deletableWsmStatuses.contains(diskStatus)
      case (true, None) => true
      case _            => false
    }
  } yield runtimeDeletable

}
