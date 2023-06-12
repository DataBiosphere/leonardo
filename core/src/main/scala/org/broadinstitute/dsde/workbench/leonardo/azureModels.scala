package org.broadinstitute.dsde.workbench.leonardo

import org.broadinstitute.dsde.workbench.azure.{
  AKSClusterName,
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

final case class PostgresName(value: String) extends AnyVal

final case class LogAnalyticsWorkspaceName(value: String) extends AnyVal

final case class LandingZoneResources(landingZoneId: UUID,
                                      clusterName: AKSClusterName,
                                      batchAccountName: BatchAccountName,
                                      relayNamespace: RelayNamespace,
                                      storageAccountName: StorageAccountName,
                                      vnetName: NetworkName,
                                      logAnalyticsWorkspaceName: LogAnalyticsWorkspaceName,
                                      batchNodesSubnetName: SubnetworkName,
                                      aksSubnetName: SubnetworkName,
                                      computeSubnetName: SubnetworkName,
                                      region: com.azure.core.management.Region,
                                      applicationInsightsName: ApplicationInsightsName
)
