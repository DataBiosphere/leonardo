package org.broadinstitute.dsde.workbench.leonardo.dao

import bio.terra.workspace.api._
import cats.effect.IO
import cats.mtl.Ask
import org.broadinstitute.dsde.workbench.leonardo.{AppContext, WorkspaceId, WsmControlledResourceId, WsmState}
import org.scalatestplus.mockito.MockitoSugar.mock

class MockWsmClientProvider(controlledAzureResourceApi: ControlledAzureResourceApi = mock[ControlledAzureResourceApi],
                            resourceApi: ResourceApi = mock[ResourceApi]
) extends WsmApiClientProvider[IO] {

  override def getControlledAzureResourceApi(token: String)(implicit
    ev: Ask[IO, AppContext]
  ): IO[ControlledAzureResourceApi] =
    IO.pure(controlledAzureResourceApi)

  override def getResourceApi(token: String)(implicit
    ev: Ask[IO, AppContext]
  ): IO[ResourceApi] =
    IO.pure(resourceApi)

  override def getDiskState(token: String, workspaceId: WorkspaceId, wsmResourceId: WsmControlledResourceId)(implicit
    ev: Ask[IO, AppContext]
  ): IO[WsmState] =
    IO.pure(WsmState(Some("READY")))

  override def getVmState(token: String, workspaceId: WorkspaceId, wsmResourceId: WsmControlledResourceId)(implicit
    ev: Ask[IO, AppContext]
  ): IO[WsmState] =
    IO.pure(WsmState(Some("READY")))

  override def getDatabaseState(token: String, workspaceId: WorkspaceId, wsmResourceId: WsmControlledResourceId)(
    implicit ev: Ask[IO, AppContext]
  ): IO[WsmState] =
    IO.pure(WsmState(Some("READY")))

  override def getNamespaceState(token: String, workspaceId: WorkspaceId, wsmResourceId: WsmControlledResourceId)(
    implicit ev: Ask[IO, AppContext]
  ): IO[WsmState] =
    IO.pure(WsmState(Some("READY")))

  override def getIdentityState(token: String, workspaceId: WorkspaceId, wsmResourceId: WsmControlledResourceId)(
    implicit ev: Ask[IO, AppContext]
  ): IO[WsmState] =
    IO.pure(WsmState(Some("READY")))
}
