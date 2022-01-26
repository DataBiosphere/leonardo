package org.broadinstitute.dsde.workbench.leonardo
package dao

import cats.effect.Async
import cats.syntax.all._
import com.azure.core.management.AzureEnvironment
import com.azure.core.management.exception.ManagementException
import com.azure.core.management.profile.AzureProfile
import com.azure.identity.{ClientSecretCredential, ClientSecretCredentialBuilder}
import com.azure.resourcemanager.compute.ComputeManager
import com.azure.resourcemanager.compute.models.VirtualMachine

//TODO: move to wbLibs
object ComputeManagerDao {
  def buildComputeManager[F[_]: Async](azureCloudContext: AzureCloudContext,
                                       azureConfig: AzureConfig): ComputeManagerDao[F] = {
    val azureCreds = getManagedAppCredentials(azureConfig)
    val azureProfile =
      new AzureProfile(azureCloudContext.tenantId.value, azureCloudContext.subscriptionId.value, AzureEnvironment.AZURE)

    val computeManager = ComputeManager.authenticate(azureCreds, azureProfile)
    new HttpComputerManagerDao(computeManager)
  }

  private def getManagedAppCredentials(azureConfig: AzureConfig): ClientSecretCredential =
    new ClientSecretCredentialBuilder()
      .clientId(azureConfig.clientId.value)
      .clientSecret(azureConfig.clientSecret.value)
      .tenantId(azureConfig.clientSecret.value)
      .build

}

trait ComputeManagerDao[F[_]] {
  def getAzureVm(name: RuntimeName, resourceGroup: ManagedResourceGroupName): F[Option[VirtualMachine]]
}

final private class HttpComputerManagerDao[F[_]](azureComputeManager: ComputeManager)(implicit val F: Async[F])
    extends ComputeManagerDao[F] {

  def getAzureVm(name: RuntimeName, resourceGroup: ManagedResourceGroupName): F[Option[VirtualMachine]] =
    for {
      vmOpt <- F
        .delay(azureComputeManager.virtualMachines().getByResourceGroup(name.asString, resourceGroup.value))
        .map(Option(_))
        .handleErrorWith {
          //TODO: this is untested
          case e: ManagementException if e.getValue.getCode().equals("ResourceNotFound") => F.pure(none[VirtualMachine])
          case e                                                                         => F.raiseError[Option[VirtualMachine]](e)
        }
    } yield vmOpt
}
