package org.broadinstitute.dsde.workbench.leonardo.dao

import cats.effect.IO
import com.azure.resourcemanager.compute.models.VirtualMachine
import org.broadinstitute.dsde.workbench.leonardo.{AzureCloudContext, RuntimeName}

class MockComputeManagerDao(vmReturn: Option[VirtualMachine] = None) extends ComputeManagerDao[IO] {
  override def getAzureVm(name: RuntimeName, cloudContext: AzureCloudContext): IO[Option[VirtualMachine]] =
    IO.pure(vmReturn)
}
