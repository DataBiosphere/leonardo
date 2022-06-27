package org.broadinstitute.dsde.workbench.leonardo
package dao

import cats.effect.Async
import cats.syntax.all._
import com.azure.core.management.AzureEnvironment
import com.azure.core.management.exception.ManagementException
import com.azure.core.management.profile.AzureProfile
import com.azure.identity.{ClientSecretCredential, ClientSecretCredentialBuilder}
import com.azure.resourcemanager.relay.RelayManager
import com.azure.resourcemanager.relay.fluent.models.AuthorizationRuleInner
import com.azure.resourcemanager.relay.models.AccessRights
import org.broadinstitute.dsde.workbench.azure.AzureAppRegistrationConfig
import org.typelevel.log4cats.StructuredLogger

import scala.jdk.CollectionConverters._

trait AzureManagerDao[F[_]] {
  def createRelayHybridConnection(relayNamespace: RelayNamespace,
                                  hybridConnectionName: RelayHybridConnectionName,
                                  cloudContext: AzureCloudContext
  ): F[PrimaryKey]
  def deleteRelayHybridConnection(relayNamespace: RelayNamespace,
                                  hybridConnectionName: RelayHybridConnectionName,
                                  cloudContext: AzureCloudContext
  ): F[Unit]
}

class AzureManagerDaoInterp[F[_]](azureConfig: AzureAppRegistrationConfig)(implicit
  val F: Async[F],
  logger: StructuredLogger[F]
) extends AzureManagerDao[F] {
  override def createRelayHybridConnection(relayNamespace: RelayNamespace,
                                           hybridConnectionName: RelayHybridConnectionName,
                                           cloudContext: AzureCloudContext
  ): F[PrimaryKey] =
    for {
      manager <- buildRelayManager(cloudContext)
      _ <- F
        .delay(
          manager
            .hybridConnections()
            .define(hybridConnectionName.value)
            .withExistingNamespace(cloudContext.managedResourceGroupName.value, relayNamespace.value)
            .withRequiresClientAuthorization(false)
            .create()
        )
        .void
        .handleErrorWith {
          case e: ManagementException if e.getValue.getCode().equals("Conflict") =>
            logger.info(s"${hybridConnectionName} already exists in ${cloudContext}")
          case e => F.raiseError[Unit](e)
        }
      _ <- F
        .delay(
          manager
            .hybridConnections()
            .createOrUpdateAuthorizationRule(
              cloudContext.managedResourceGroupName.value,
              relayNamespace.value,
              hybridConnectionName.value,
              "listener",
              new AuthorizationRuleInner().withRights(List(AccessRights.LISTEN).asJava)
            )
        )
      key <- F
        .delay(
          manager
            .hybridConnections()
            .listKeys(cloudContext.managedResourceGroupName.value,
                      relayNamespace.value,
                      hybridConnectionName.value,
                      "listener"
            )
            .primaryKey()
        )
    } yield PrimaryKey(key)

  override def deleteRelayHybridConnection(relayNamespace: RelayNamespace,
                                           hybridConnectionName: RelayHybridConnectionName,
                                           cloudContext: AzureCloudContext
  ): F[Unit] =
    for {
      manager <- buildRelayManager(cloudContext)
      _ <- F
        .delay(
          manager
            .hybridConnections()
            .delete(cloudContext.managedResourceGroupName.value, relayNamespace.value, hybridConnectionName.value)
        )
    } yield ()

  private def buildAzureProfile(azureCloudContext: AzureCloudContext): (ClientSecretCredential, AzureProfile) = {
    val azureCreds = new ClientSecretCredentialBuilder()
      .clientId(azureConfig.clientId.value)
      .clientSecret(azureConfig.clientSecret.value)
      .tenantId(azureConfig.managedAppTenantId.value)
      .build
    val azureProfile =
      new AzureProfile(azureCloudContext.tenantId.value, azureCloudContext.subscriptionId.value, AzureEnvironment.AZURE)
    (azureCreds, azureProfile)
  }

  private def buildRelayManager(azureCloudContext: AzureCloudContext): F[RelayManager] = {
    val (azureCreds, azureProfile) = buildAzureProfile(azureCloudContext)

    F.delay(RelayManager.authenticate(azureCreds, azureProfile))
  }
}

final case class PrimaryKey(value: String) extends AnyVal
