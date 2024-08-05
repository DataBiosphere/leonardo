package org.broadinstitute.dsde.workbench.leonardo.dao

import bio.terra.workspace.api._
import bio.terra.workspace.model.{IamRole, ResourceMetadata}
import cats.effect.IO
import cats.mtl.Ask
import org.broadinstitute.dsde.workbench.azure.{AzureCloudContext, ManagedResourceGroupName, SubscriptionId, TenantId}
import org.broadinstitute.dsde.workbench.leonardo.db.WsmResourceType
import org.broadinstitute.dsde.workbench.leonardo.{AppContext, WorkspaceId, WsmControlledResourceId, WsmState}
import org.scalatestplus.mockito.MockitoSugar.mock
import org.typelevel.log4cats.StructuredLogger

class MockWsmClientProvider(controlledAzureResourceApi: ControlledAzureResourceApi = mock[ControlledAzureResourceApi],
                            resourceApi: ResourceApi = mock[ResourceApi],
                            workspaceApi: WorkspaceApi = mock[WorkspaceApi]
) extends WsmApiClientProvider[IO] {

  override def getWorkspace(token: String, workspaceId: WorkspaceId, iamRole: IamRole)(implicit
    ev: Ask[IO, AppContext]
  ): IO[Option[WorkspaceDescription]] = IO.pure(
    Some(
      WorkspaceDescription(
        workspaceId,
        "workspaceName" + workspaceId,
        "spend-profile",
        Some(
          AzureCloudContext(TenantId(workspaceId.toString),
                            SubscriptionId(workspaceId.toString),
                            ManagedResourceGroupName(workspaceId.toString)
          )
        ),
        None
      )
    )
  )

  override def getControlledAzureResourceApi(token: String)(implicit
    ev: Ask[IO, AppContext]
  ): IO[ControlledAzureResourceApi] =
    IO.pure(controlledAzureResourceApi)

  override def getResourceApi(token: String)(implicit
    ev: Ask[IO, AppContext]
  ): IO[ResourceApi] =
    IO.pure(resourceApi)

  override def getWorkspaceApi(token: String)(implicit
    ev: Ask[IO, AppContext]
  ): IO[WorkspaceApi] =
    IO.pure(workspaceApi)

  override def getIdentity(token: String, workspaceId: WorkspaceId, wsmResourceId: WsmControlledResourceId)(implicit
    ev: Ask[IO, AppContext],
    log: StructuredLogger[IO]
  ): IO[Option[ResourceMetadata]] = IO.pure(None)

  override def getVm(token: String, workspaceId: WorkspaceId, wsmResourceId: WsmControlledResourceId)(implicit
    ev: Ask[IO, AppContext],
    log: StructuredLogger[IO]
  ): IO[Option[ResourceMetadata]] = IO.pure(None)

  override def getDatabase(token: String, workspaceId: WorkspaceId, wsmResourceId: WsmControlledResourceId)(implicit
    ev: Ask[IO, AppContext],
    log: StructuredLogger[IO]
  ): IO[Option[ResourceMetadata]] = IO.pure(None)

  override def getNamespace(token: String, workspaceId: WorkspaceId, wsmResourceId: WsmControlledResourceId)(implicit
    ev: Ask[IO, AppContext],
    log: StructuredLogger[IO]
  ): IO[Option[ResourceMetadata]] = IO.pure(None)

  override def getDisk(token: String, workspaceId: WorkspaceId, wsmResourceId: WsmControlledResourceId)(implicit
    ev: Ask[IO, AppContext],
    log: StructuredLogger[IO]
  ): IO[Option[ResourceMetadata]] = IO.pure(None)

  override def getWsmState(token: String,
                           workspaceId: WorkspaceId,
                           wsmResourceId: WsmControlledResourceId,
                           resourceType: WsmResourceType
  )(implicit
    ev: Ask[IO, AppContext],
    log: StructuredLogger[IO]
  ): IO[WsmState] =
    IO.pure(WsmState(Some("READY")))

}
