package org.broadinstitute.dsde.workbench.leonardo.dao

import bio.terra.workspace.api._
import cats.effect.IO
import cats.mtl.Ask
import org.broadinstitute.dsde.workbench.leonardo.AppContext
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
}
