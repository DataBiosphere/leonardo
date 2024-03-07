package org.broadinstitute.dsde.workbench.leonardo.config

import com.azure.core.management.AzureEnvironment
import org.broadinstitute.dsde.workbench.azure.{AzureServiceBusPublisherConfig, AzureServiceBusSubscriberConfig}

case class AzureHostingModeConfig(
  enabled: Boolean = false, // if true, the app will use Azure for hosting, default is false (GCP)
  azureEnvironment: String = AzureEnvironmentConverter.Azure,
  managedIdentityAuthConfig: AzureManagedIdentityAuthConfig,
  publisherConfig: AzureServiceBusPublisherConfig,
  subscriberConfig: AzureServiceBusSubscriberConfig
)

case class AzureManagedIdentityAuthConfig(
  tokenScope: String,
  tokenAcquisitionTimeout: Int = 30 // in seconds
)

object AzureEnvironmentConverter {
  val Azure: String = "AZURE"
  val AzureGov: String = "AZURE_GOV"

  def fromString(s: String): AzureEnvironment = s match {
    case AzureGov => AzureEnvironment.AZURE_US_GOVERNMENT
    // a bit redundant, but I want to have a explicit case for Azure for clarity, even though it's the default
    case Azure => AzureEnvironment.AZURE
    case _     => AzureEnvironment.AZURE
  }
}
