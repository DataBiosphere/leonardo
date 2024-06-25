package org.broadinstitute.dsde.workbench.leonardo.config

import org.broadinstitute.dsde.workbench.google2.KubernetesSerializableName.ServiceAccountName
import org.broadinstitute.dsde.workbench.leonardo.AppType._
import org.broadinstitute.dsde.workbench.leonardo._
import org.broadinstitute.dsde.workbench.leonardo.http.ConfigReader
import org.broadinstitute.dsp.{ChartName, ChartVersion}

import java.net.URL

sealed trait KubernetesAppConfig extends Product with Serializable {
  def chartName: ChartName

  def chartVersion: ChartVersion

  def releaseNameSuffix: ReleaseNameSuffix

  def namespaceNameSuffix: NamespaceNameSuffix

  def serviceAccountName: ServiceAccountName

  def chart: Chart = Chart(chartName, chartVersion)

  def kubernetesServices: List[KubernetesService]

  def enabled: Boolean

  // Each app can configure its own list of chart versions to NEVER update. To be used
  // when changes aren't backward-compatible and we know an update would be destructive.
  def chartVersionsToExcludeFromUpdates: List[ChartVersion]

  // These are defined by each implementing class. Each config type
  // corresponds to a specific app type and cloud provider.
  def cloudProvider: CloudProvider
  def appType: AppType
}

object KubernetesAppConfig {
  def configForTypeAndCloud(appType: AppType, cloudProvider: CloudProvider): Option[KubernetesAppConfig] =
    (appType, cloudProvider) match {
      case (Galaxy, CloudProvider.Gcp)              => Some(Config.gkeGalaxyAppConfig)
      case (Custom, CloudProvider.Gcp)              => Some(Config.gkeCustomAppConfig)
      case (Cromwell, CloudProvider.Gcp)            => Some(Config.gkeCromwellAppConfig)
      case (AppType.Allowed, CloudProvider.Gcp)     => Some(Config.gkeAllowedAppConfig)
      case (Cromwell, CloudProvider.Azure)          => Some(ConfigReader.appConfig.azure.coaAppConfig)
      case (WorkflowsApp, CloudProvider.Azure)      => Some(ConfigReader.appConfig.azure.workflowsAppConfig)
      case (CromwellRunnerApp, CloudProvider.Azure) => Some(ConfigReader.appConfig.azure.cromwellRunnerAppConfig)
      case (Wds, CloudProvider.Azure)               => Some(ConfigReader.appConfig.azure.wdsAppConfig)
      case (HailBatch, CloudProvider.Azure)         => Some(ConfigReader.appConfig.azure.hailBatchAppConfig)
      case _                                        => None
    }
}

final case class GalaxyAppConfig(releaseNameSuffix: ReleaseNameSuffix,
                                 chartName: ChartName,
                                 chartVersion: ChartVersion,
                                 namespaceNameSuffix: NamespaceNameSuffix,
                                 services: List[ServiceConfig],
                                 serviceAccountName: ServiceAccountName,
                                 uninstallKeepHistory: Boolean,
                                 postgresPassword: DbPassword,
                                 orchUrl: GalaxyOrchUrl,
                                 drsUrl: GalaxyDrsUrl,
                                 minMemoryGb: Int,
                                 minNumOfCpus: Int,
                                 enabled: Boolean,
                                 chartVersionsToExcludeFromUpdates: List[ChartVersion]
) extends KubernetesAppConfig {
  override val kubernetesServices: List[KubernetesService] = services.map(s => KubernetesService(ServiceId(-1), s))

  val cloudProvider: CloudProvider = CloudProvider.Gcp
  val appType: AppType = AppType.Galaxy
}

final case class CromwellAppConfig(chartName: ChartName,
                                   chartVersion: ChartVersion,
                                   namespaceNameSuffix: NamespaceNameSuffix,
                                   releaseNameSuffix: ReleaseNameSuffix,
                                   services: List[ServiceConfig],
                                   serviceAccountName: ServiceAccountName,
                                   dbPassword: DbPassword,
                                   enabled: Boolean,
                                   chartVersionsToExcludeFromUpdates: List[ChartVersion]
) extends KubernetesAppConfig {
  override val kubernetesServices: List[KubernetesService] = services.map(s => KubernetesService(ServiceId(-1), s))

  val cloudProvider: CloudProvider = CloudProvider.Gcp
  val appType: AppType = AppType.Cromwell
}

final case class CustomApplicationAllowListConfig(default: List[String], highSecurity: List[String])

final case class CustomAppConfig(chartName: ChartName,
                                 chartVersion: ChartVersion,
                                 releaseNameSuffix: ReleaseNameSuffix,
                                 namespaceNameSuffix: NamespaceNameSuffix,
                                 serviceAccountName: ServiceAccountName,
                                 customApplicationAllowList: CustomApplicationAllowListConfig,
                                 enabled: Boolean,
                                 chartVersionsToExcludeFromUpdates: List[ChartVersion]
) extends KubernetesAppConfig {
  // Not known at config. Generated at runtime.
  override val kubernetesServices: List[KubernetesService] = List.empty

  val cloudProvider: CloudProvider = CloudProvider.Gcp
  val appType: AppType = AppType.Custom
}

final case class CoaAppConfig(chartName: ChartName,
                              chartVersion: ChartVersion,
                              releaseNameSuffix: ReleaseNameSuffix,
                              namespaceNameSuffix: NamespaceNameSuffix,
                              ksaName: KsaName,
                              services: List[ServiceConfig],
                              instrumentationEnabled: Boolean,
                              enabled: Boolean,
                              dockstoreBaseUrl: URL,
                              databaseEnabled: Boolean,
                              chartVersionsToExcludeFromUpdates: List[ChartVersion]
) extends KubernetesAppConfig {
  override val kubernetesServices: List[KubernetesService] = services.map(s => KubernetesService(ServiceId(-1), s))

  override val serviceAccountName = ServiceAccountName(ksaName.value)

  val cloudProvider: CloudProvider = CloudProvider.Azure
  val appType: AppType = AppType.Cromwell
}

final case class WorkflowsAppConfig(chartName: ChartName,
                                    chartVersion: ChartVersion,
                                    releaseNameSuffix: ReleaseNameSuffix,
                                    namespaceNameSuffix: NamespaceNameSuffix,
                                    ksaName: KsaName,
                                    services: List[ServiceConfig],
                                    instrumentationEnabled: Boolean,
                                    enabled: Boolean,
                                    dockstoreBaseUrl: URL,
                                    chartVersionsToExcludeFromUpdates: List[ChartVersion],
                                    ecmBaseUri: URL,
                                    bardBaseUri: URL,
                                    bardEnabled: Boolean
) extends KubernetesAppConfig {
  override lazy val kubernetesServices: List[KubernetesService] = services.map(s => KubernetesService(ServiceId(-1), s))
  override val serviceAccountName = ServiceAccountName(ksaName.value)

  val cloudProvider: CloudProvider = CloudProvider.Azure
  val appType: AppType = AppType.WorkflowsApp
}

final case class CromwellRunnerAppConfig(chartName: ChartName,
                                         chartVersion: ChartVersion,
                                         releaseNameSuffix: ReleaseNameSuffix,
                                         namespaceNameSuffix: NamespaceNameSuffix,
                                         ksaName: KsaName,
                                         services: List[ServiceConfig],
                                         instrumentationEnabled: Boolean,
                                         enabled: Boolean,
                                         chartVersionsToExcludeFromUpdates: List[ChartVersion],
                                         ecmBaseUri: URL,
                                         bardBaseUri: URL,
                                         bardEnabled: Boolean
) extends KubernetesAppConfig {
  override lazy val kubernetesServices: List[KubernetesService] = services.map(s => KubernetesService(ServiceId(-1), s))
  override val serviceAccountName = ServiceAccountName(ksaName.value)
  val cloudProvider: CloudProvider = CloudProvider.Azure
  val appType: AppType = AppType.CromwellRunnerApp
}

final case class WdsAppConfig(chartName: ChartName,
                              chartVersion: ChartVersion,
                              releaseNameSuffix: ReleaseNameSuffix,
                              namespaceNameSuffix: NamespaceNameSuffix,
                              ksaName: KsaName,
                              services: List[ServiceConfig],
                              instrumentationEnabled: Boolean,
                              enabled: Boolean,
                              databaseEnabled: Boolean,
                              environment: String,
                              environmentBase: String,
                              chartVersionsToExcludeFromUpdates: List[ChartVersion]
) extends KubernetesAppConfig {
  override lazy val kubernetesServices: List[KubernetesService] = services.map(s => KubernetesService(ServiceId(-1), s))
  override val serviceAccountName = ServiceAccountName(ksaName.value)

  val cloudProvider: CloudProvider = CloudProvider.Azure
  val appType: AppType = AppType.Wds
}

final case class HailBatchAppConfig(chartName: ChartName,
                                    chartVersion: ChartVersion,
                                    releaseNameSuffix: ReleaseNameSuffix,
                                    namespaceNameSuffix: NamespaceNameSuffix,
                                    ksaName: KsaName,
                                    services: List[ServiceConfig],
                                    enabled: Boolean,
                                    chartVersionsToExcludeFromUpdates: List[ChartVersion]
) extends KubernetesAppConfig {
  override val kubernetesServices: List[KubernetesService] = services.map(s => KubernetesService(ServiceId(-1), s))
  override val serviceAccountName = ServiceAccountName(ksaName.value)

  val cloudProvider: CloudProvider = CloudProvider.Azure
  val appType: AppType = AppType.HailBatch
}

final case class ContainerRegistryUsername(asString: String) extends AnyVal
final case class ContainerRegistryPassword(asString: String) extends AnyVal
final case class ContainerRegistryCredentials(username: ContainerRegistryUsername, password: ContainerRegistryPassword)
final case class AllowedAppConfig(chartName: ChartName,
                                  rstudioChartVersion: ChartVersion,
                                  sasChartVersion: ChartVersion,
                                  namespaceNameSuffix: NamespaceNameSuffix,
                                  releaseNameSuffix: ReleaseNameSuffix,
                                  services: List[ServiceConfig],
                                  serviceAccountName: ServiceAccountName,
                                  sasContainerRegistryCredentials: ContainerRegistryCredentials,
                                  chartVersionsToExcludeFromUpdates: List[ChartVersion],
                                  numOfReplicas: Int
) extends KubernetesAppConfig {
  val cloudProvider: CloudProvider = CloudProvider.Gcp
  val appType: AppType = AppType.Allowed

  override val kubernetesServices: List[KubernetesService] = services.map(s => KubernetesService(ServiceId(-1), s))
  def enabled: Boolean = true

  def chartVersion: ChartVersion = ChartVersion(
    "dummy"
  ) // For AoU apps, chart version will vary, and will be populated from user request
}
