package org.broadinstitute.dsde.workbench.leonardo
package dao

import cats.effect.Async
import cats.syntax.all._
import com.azure.core.management.AzureEnvironment
import com.azure.core.management.exception.ManagementException
import com.azure.core.management.profile.AzureProfile
import com.azure.identity.ClientSecretCredentialBuilder
import com.azure.resourcemanager.compute.ComputeManager
import com.azure.resourcemanager.compute.models.VirtualMachine

trait ComputeManagerDao[F[_]] {
  def getAzureVm(name: RuntimeName, cloudContext: AzureCloudContext): F[Option[VirtualMachine]]
}

class HttpComputerManagerDao[F[_]](azureConfig: AzureAppRegistrationConfig)(implicit val F: Async[F])
    extends ComputeManagerDao[F] {

  def getAzureVm(name: RuntimeName, cloudContext: AzureCloudContext): F[Option[VirtualMachine]] =
    for {
      azureComputeManager <- F.pure(buildComputeManager(cloudContext, azureConfig))
      vmOpt <- F
        .delay(
          azureComputeManager
            .virtualMachines()
            .getByResourceGroup(cloudContext.managedResourceGroupName.value, name.asString)
        )
        .map(Option(_))
        .handleErrorWith {
          case e: ManagementException if e.getValue.getCode().equals("ResourceNotFound") => F.pure(none[VirtualMachine])
          case e                                                                         => F.raiseError[Option[VirtualMachine]](e)
        }
    } yield vmOpt

  private def buildComputeManager(azureCloudContext: AzureCloudContext,
                                  azureConfig: AzureAppRegistrationConfig
  ): ComputeManager = {
    val azureCreds = new ClientSecretCredentialBuilder()
      .clientId(azureConfig.clientId.value)
      .clientSecret(azureConfig.clientSecret.value)
      .tenantId(azureConfig.managedAppTenantId.value)
      .build
    val azureProfile =
      new AzureProfile(azureCloudContext.tenantId.value, azureCloudContext.subscriptionId.value, AzureEnvironment.AZURE)

    ComputeManager.authenticate(azureCreds, azureProfile)
  }
}
