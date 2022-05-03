package org.broadinstitute.dsde.workbench.leonardo.dao

import cats.effect.IO
import com.azure.resourcemanager.compute.models.VirtualMachine
import org.broadinstitute.dsde.workbench.leonardo.{
  AzureCloudContext,
  RelayHybridConnectionName,
  RelayNamespace,
  RuntimeName
}

class MockComputeManagerDao(vmReturn: Option[VirtualMachine] = None) extends AzureManagerDao[IO] {
  override def getAzureVm(name: RuntimeName, cloudContext: AzureCloudContext): IO[Option[VirtualMachine]] =
    IO.pure(vmReturn)

  override def createRelayHybridConnection(relayNamespace: RelayNamespace,
                                           hybridConnectionName: RelayHybridConnectionName,
                                           cloudContext: AzureCloudContext): IO[PrimaryKey] = IO.pure(PrimaryKey("key"))

  override def deleteRelayHybridConnection(relayNamespace: RelayNamespace,
                                           hybridConnectionName: RelayHybridConnectionName,
                                           cloudContext: AzureCloudContext): IO[Unit] = IO.unit
}
