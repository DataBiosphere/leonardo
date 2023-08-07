package org.broadinstitute.dsde.workbench.leonardo
package http

import org.broadinstitute.dsde.workbench.azure.AzureAppRegistrationConfig
import org.broadinstitute.dsde.workbench.leonardo.config._
import org.broadinstitute.dsde.workbench.leonardo.util.{AzurePubsubHandlerConfig, TerraAppSetupChartConfig}
import org.broadinstitute.dsp.{ChartName, ChartVersion, Namespace, Release, Values}
import org.http4s.Uri
import pureconfig.ConfigSource
import _root_.pureconfig.generic.auto._
import ConfigImplicits._
import org.broadinstitute.dsde.workbench.leonardo.monitor.LeoMetricsMonitorConfig

object ConfigReader {
  lazy val appConfig =
    ConfigSource
      .fromConfig(org.broadinstitute.dsde.workbench.leonardo.config.Config.config)
      .loadOrThrow[AppConfig]

  // Encapsulates config for all apps Leo administers
  lazy val adminAppConfig =
    AdminAppConfig(
      appConfig.azure.coaAppConfig,
      Config.gkeCromwellAppConfig,
      Config.gkeCustomAppConfig,
      Config.gkeGalaxyAppConfig,
      appConfig.azure.hailBatchAppConfig,
      Config.gkeRStudioAppConfig,
      appConfig.azure.wdsAppConfig
    )
}

final case class AzureConfig(pubsubHandler: AzurePubsubHandlerConfig,
                             wsm: HttpWsmDaoConfig,
                             appRegistration: AzureAppRegistrationConfig,
                             coaAppConfig: CoaAppConfig,
                             wdsAppConfig: WdsAppConfig,
                             hailBatchAppConfig: HailBatchAppConfig,
                             aadPodIdentityConfig: AadPodIdentityConfig,
                             allowedSharedApps: List[String],
                             tdr: TdrConfig,
relayListenerChartConfig: RelayListenerChartConfig
)

final case class OidcAuthConfig(
  authorityEndpoint: Uri,
  clientId: org.broadinstitute.dsde.workbench.oauth2.ClientId,
  clientSecret: Option[org.broadinstitute.dsde.workbench.oauth2.ClientSecret],
  legacyGoogleClientId: org.broadinstitute.dsde.workbench.oauth2.ClientId
)

final case class AadPodIdentityConfig(namespace: Namespace,
                                      release: Release,
                                      chartName: ChartName,
                                      chartVersion: ChartVersion,
                                      values: Values
)

final case class DrsConfig(url: String)

final case class TdrConfig(url: String)

final case class RelayListenerChartConfig(chartName: ChartName, chartVersion: ChartVersion)

// Note: pureconfig supports reading kebab case into camel case in code by default
// More docs see https://pureconfig.github.io/docs/index.html
final case class AppConfig(terraAppSetupChart: TerraAppSetupChartConfig,
                           persistentDisk: PersistentDiskConfig,
                           azure: AzureConfig,
                           oidc: OidcAuthConfig,
                           drs: DrsConfig,
                           metrics: LeoMetricsMonitorConfig
)

final case class AdminAppConfig(coaAppConfig: CoaAppConfig,
                                cromwellAppConfig: CromwellAppConfig,
                                customAppConfig: CustomAppConfig,
                                galaxyAppConfig: GalaxyAppConfig,
                                hailBatchAppConfig: HailBatchAppConfig,
                                rstudioAppConfig: RStudioAppConfig,
                                wdsAppConfig: WdsAppConfig
) {

  def configForTypeAndCloud(appType: AppType, cloudProvider: CloudProvider): Option[KubernetesAppConfig] =
    asList.find(c => c.appType == appType && c.cloudProvider == cloudProvider)

  private lazy val asList: List[KubernetesAppConfig] = List(
    coaAppConfig,
    cromwellAppConfig,
    customAppConfig,
    galaxyAppConfig,
    hailBatchAppConfig,
    rstudioAppConfig,
    wdsAppConfig
  )
}
