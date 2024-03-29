package org.broadinstitute.dsde.workbench.leonardo

import org.broadinstitute.dsde.workbench.azure.{
  AKSClusterName,
  ApplicationInsightsName,
  BatchAccountName,
  RelayNamespace
}
import org.broadinstitute.dsde.workbench.google2.KubernetesSerializableName.{NamespaceName, ServiceAccountName}
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

final case class AKSCluster(name: String, tags: Map[String, Boolean]) {
  def asClusterName: AKSClusterName = AKSClusterName(name)
}

final case class WsmManagedAzureIdentity(wsmResourceName: String, managedIdentityName: String)

final case class WsmControlledDatabaseResource(wsmDatabaseName: String,
                                               azureDatabaseName: String,
                                               controlledResourceId: UUID = null
)

final case class WsmControlledKubernetesNamespaceResource(name: NamespaceName,
                                                          wsmResourceId: WsmControlledResourceId,
                                                          serviceAccountName: ServiceAccountName
)

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
