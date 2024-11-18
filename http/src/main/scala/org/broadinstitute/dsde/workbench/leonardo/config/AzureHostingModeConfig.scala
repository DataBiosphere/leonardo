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
  val Azure: String = "AzureCloud"
  val AzureGov: String = "AzureUSGovernmentCloud"

  def fromString(s: String): AzureEnvironment = s match {
    case AzureGov => AzureEnvironment.AZURE_US_GOVERNMENT
    // a bit redundant, but I want to have a explicit case for Azure for clarity, even though it's the default
    case Azure => AzureEnvironment.AZURE
    case _     => AzureEnvironment.AZURE
  }

  // servicebus suffix not currently provided by AzureEnvironment library, values found here
  def relaySuffixFromEnvironment(azureEnvironment: AzureEnvironment): String = azureEnvironment match {
    case AzureEnvironment.AZURE_US_GOVERNMENT => ".servicebus.usgovcloudapi.net"
    // a bit redundant, but I want to have a explicit case for Azure for clarity, even though it's the default
    case AzureEnvironment.AZURE => ".servicebus.windows.net"
    case _                      => ".servicebus.windows.net"
  }

  def relaySuffixFromString(s: String): String =
    relaySuffixFromEnvironment(fromString(s))

  // database suffix not currently provided by AzureEnvironment library, values found here
  def postgresSuffixFromEnvironment(azureEnvironment: AzureEnvironment): String = azureEnvironment match {
    case AzureEnvironment.AZURE_US_GOVERNMENT => ".database.usgovcloudapi.net"
    // a bit redundant, but I want to have a explicit case for Azure for clarity, even though it's the default
    case AzureEnvironment.AZURE => ".database.azure.com"
    case _                      => ".database.azure.com"
  }

  def postgresSuffixFromString(s: String): String =
    postgresSuffixFromEnvironment(fromString(s))

  // batchAccount suffix not currently provided by AzureEnvironment library, values found here
  def batchAccountSuffixFromEnvironment(azureEnvironment: AzureEnvironment): String = azureEnvironment match {
    case AzureEnvironment.AZURE_US_GOVERNMENT => ".batch.usgovcloudapi.net"
    // a bit redundant, but I want to have a explicit case for Azure for clarity, even though it's the default
    case AzureEnvironment.AZURE => ".batch.azure.com"
    case _                      => ".batch.azure.com"
  }

  def batchAccountSuffixFromString(s: String): String =
    postgresSuffixFromEnvironment(fromString(s))
}
