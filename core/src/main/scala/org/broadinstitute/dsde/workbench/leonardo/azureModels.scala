package org.broadinstitute.dsde.workbench.leonardo

import org.broadinstitute.dsde.workbench.azure.{
  ApplicationInsightsName,
  BatchAccountName,
  RelayNamespace
}

import org.broadinstitute.dsde.workbench.google2.{NetworkName, SubnetworkName}

import java.util.UUID

final case class WsmControlledResourceId(value: UUID) extends AnyVal

final case class AzureUnimplementedException(message: String) extends Exception {
  override def getMessage: String = message
}

final case class StorageAccountName(value: String) extends AnyVal

final case class WsmJobId(value: String) extends AnyVal

final case class ManagedIdentityName(value: String) extends AnyVal

final case class BatchAccountKey(value: String) extends AnyVal

final case class PostgresServer(name: String, pgBouncerEnabled: Boolean)

final case class AKSCluster(name: String, tags: Map[String, Boolean])

final case class WsmManagedAzureIdentity(wsmResourceName: String, managedIdentityName: String)

final case class WsmControlledDatabaseResource(wsmDatabaseName: String, azureDatabaseName: String)

final case class LandingZoneResources(landingZoneId: UUID,
                                      aksCluster: AKSCluster,
                                      batchAccountName: BatchAccountName,
                                      relayNamespace: RelayNamespace,
                                      storageAccountName: StorageAccountName,
                                      vnetName: NetworkName,
                                      batchNodesSubnetName: SubnetworkName,
                                      aksSubnetName: SubnetworkName,
                                      region: com.azure.core.management.Region,
                                      applicationInsightsName: ApplicationInsightsName,
                                      postgresServer: Option[PostgresServer]
)
