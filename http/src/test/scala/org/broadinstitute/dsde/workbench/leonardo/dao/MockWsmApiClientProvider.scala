package org.broadinstitute.dsde.workbench.leonardo.dao

import bio.terra.workspace.api._
import org.scalatestplus.mockito.MockitoSugar.mock

class MockWsmClientProvider(controlledAzureResourceApi: ControlledAzureResourceApi = mock[ControlledAzureResourceApi],
                            resourceApi: ResourceApi = mock[ResourceApi]
) extends WsmApiClientProvider {

  override def getControlledAzureResourceApi(token: String): ControlledAzureResourceApi =
    controlledAzureResourceApi

  override def getResourceApi(token: String): ResourceApi = resourceApi
}
