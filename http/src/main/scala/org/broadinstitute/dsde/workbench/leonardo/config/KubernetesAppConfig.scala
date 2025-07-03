package org.broadinstitute.dsde.workbench.leonardo.config

import org.broadinstitute.dsde.workbench.google2.KubernetesSerializableName.ServiceAccountName
import org.broadinstitute.dsde.workbench.leonardo.AppType._
import org.broadinstitute.dsde.workbench.leonardo._
import org.broadinstitute.dsp.{ChartName, ChartVersion}

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
                                   backend: CromwellBackendName,
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
