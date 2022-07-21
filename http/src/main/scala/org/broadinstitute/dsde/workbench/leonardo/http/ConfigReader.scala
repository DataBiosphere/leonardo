package org.broadinstitute.dsde.workbench.leonardo
package http

import pureconfig.ConfigSource
import _root_.pureconfig.generic.auto._
import ConfigImplicits._
import org.broadinstitute.dsde.workbench.azure.AzureAppRegistrationConfig
import org.broadinstitute.dsde.workbench.leonardo.util.{AzurePubsubHandlerConfig, TerraAppSetupChartConfig}
import org.broadinstitute.dsde.workbench.leonardo.config.{HttpWsmDaoConfig, PersistentDiskConfig}
import org.http4s.Uri

object ConfigReader {
  lazy val appConfig =
    ConfigSource
      .fromConfig(org.broadinstitute.dsde.workbench.leonardo.config.Config.config)
      .loadOrThrow[AppConfig]
}

final case class AzureConfig(
  pubsubHandler: AzurePubsubHandlerConfig,
  wsm: HttpWsmDaoConfig,
  appRegistration: AzureAppRegistrationConfig
)

final case class OidcAuthConfig(
  authorityEndpoint: Uri,
  clientId: org.broadinstitute.dsde.workbench.oauth2.ClientId,
  clientSecret: Option[org.broadinstitute.dsde.workbench.oauth2.ClientSecret],
  legacyGoogleClientId: org.broadinstitute.dsde.workbench.oauth2.ClientId
)

// Note: pureconfig supports reading kebab case into camel case in code by default
// More docs see https://pureconfig.github.io/docs/index.html
final case class AppConfig(
  terraAppSetupChart: TerraAppSetupChartConfig,
  persistentDisk: PersistentDiskConfig,
  azure: AzureConfig,
  oidc: OidcAuthConfig
)
