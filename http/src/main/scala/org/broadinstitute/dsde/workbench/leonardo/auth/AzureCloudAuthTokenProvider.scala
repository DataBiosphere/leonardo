package org.broadinstitute.dsde.workbench.leonardo.auth

import cats.effect._
import cats.syntax.all._
import com.azure.core.credential.TokenRequestContext
import com.azure.identity.DefaultAzureCredentialBuilder
import org.broadinstitute.dsde.workbench.leonardo.CloudProvider
import org.broadinstitute.dsde.workbench.leonardo.config.{AzureEnvironmentConverter, AzureHostingModeConfig}

import java.time.Duration

/***
 * AzureCloudAuthTokenProvider is a CloudServiceAuthTokenProvider for Azure.
 * It uses the DefaultAzureCredentialBuilder to get a token for the managed identity.
 * Supports multiple Azure environments.
 */
class AzureCloudAuthTokenProvider[F[_]](config: AzureHostingModeConfig)(implicit F: Async[F])
    extends CloudServiceAuthTokenProvider[F](CloudProvider.Azure) {

  private val credentialBuilder: DefaultAzureCredentialBuilder =
    new DefaultAzureCredentialBuilder()
      .authorityHost(
        AzureEnvironmentConverter
          .fromString(config.azureEnvironment)
          .getActiveDirectoryEndpoint
      )

  private val tokenRequestContext: TokenRequestContext = {
    val trc = new TokenRequestContext()
    trc.addScopes(config.managedIdentityAuthConfig.tokenScope)
    trc
  }

  override def getCloudProviderAuthToken: F[CloudToken] = {
    val token = getManagedIdentityAccessToken
    token.pure[F]
  }
  private def getManagedIdentityAccessToken: CloudToken = {
    // The desired client id can be set using the env variable AZURE_CLIENT_ID.
    // If not set, the client ID of the system assigned managed identity will be used.
    val credentials = credentialBuilder
      .build()

    val token = credentials
      .getToken(tokenRequestContext)
      .block(Duration.ofSeconds(config.managedIdentityAuthConfig.tokenAcquisitionTimeout))

    CloudToken(token.getToken, token.getExpiresAt.toInstant)
  }
}
