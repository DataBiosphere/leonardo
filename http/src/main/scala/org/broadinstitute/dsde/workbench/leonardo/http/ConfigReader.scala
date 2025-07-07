package org.broadinstitute.dsde.workbench.leonardo
package http

import _root_.pureconfig.generic.auto._
import org.broadinstitute.dsde.workbench.azure.AzureAppRegistrationConfig
import org.broadinstitute.dsde.workbench.google2.KubernetesSerializableName.ServiceName
import org.broadinstitute.dsde.workbench.leonardo.ConfigImplicits._
import org.broadinstitute.dsde.workbench.leonardo.config._
import org.broadinstitute.dsde.workbench.leonardo.monitor.LeoMetricsMonitorConfig
import org.broadinstitute.dsde.workbench.leonardo.util.TerraAppSetupChartConfig
import org.broadinstitute.dsp.{ChartName, ChartVersion}
import org.http4s.Uri
import pureconfig.ConfigSource

object ConfigReader {
  lazy val appConfig =
    ConfigSource
      .fromConfig(org.broadinstitute.dsde.workbench.leonardo.config.Config.config)
      .loadOrThrow[AppConfig]
}
final case class AzureConfig(
  wsm: HttpWsmDaoConfig,
  bpm: BpmConfig,
  appRegistration: AzureAppRegistrationConfig,
  allowedSharedApps: List[AppType],
  tdr: TdrConfig,
  listenerChartConfig: ListenerChartConfig,
  hostingModeConfig: AzureHostingModeConfig
)

final case class OidcAuthConfig(
  authorityEndpoint: Uri,
  clientId: org.broadinstitute.dsde.workbench.oauth2.ClientId
)

final case class DrsConfig(url: String)

final case class TdrConfig(url: String)

final case class ListenerChartConfig(chartName: ChartName, chartVersion: ChartVersion) {
  def service = KubernetesService(
    ServiceId(-1),
    ServiceConfig(ServiceName("listener"), KubernetesServiceKindName("ClusterIP"))
  )
}

// Note: pureconfig supports reading kebab case into camel case in code by default
// More docs see https://pureconfig.github.io/docs/index.html
final case class AppConfig(
  terraAppSetupChart: TerraAppSetupChartConfig,
  persistentDisk: PersistentDiskConfig,
  azure: AzureConfig,
  oidc: OidcAuthConfig,
  drs: DrsConfig,
  metrics: LeoMetricsMonitorConfig
)
