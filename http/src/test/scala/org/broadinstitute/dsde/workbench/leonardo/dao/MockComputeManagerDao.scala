package org.broadinstitute.dsde.workbench.leonardo.dao

import cats.effect.IO
import com.azure.resourcemanager.compute.models.VirtualMachine
import org.broadinstitute.dsde.workbench.leonardo.{RuntimeName, ManagedResourceGroupName}

class MockComputeManagerDao extends ComputeManagerDao[IO] {
  override def getAzureVm(name: RuntimeName, resourceGroup: ManagedResourceGroupName): IO[Option[VirtualMachine]] = IO.pure(None)
}
