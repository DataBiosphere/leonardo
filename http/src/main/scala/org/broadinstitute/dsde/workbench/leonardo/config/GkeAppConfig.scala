package org.broadinstitute.dsde.workbench.leonardo.config

import org.broadinstitute.dsde.workbench.google2.KubernetesSerializableName.ServiceAccountName
import org.broadinstitute.dsde.workbench.leonardo.{
  Chart,
  DbPassword,
  GalaxyDrsUrl,
  GalaxyOrchUrl,
  KubernetesService,
  NamespaceNameSuffix,
  ReleaseNameSuffix,
  ServiceConfig,
  ServiceId
}
import org.broadinstitute.dsp.{ChartName, ChartVersion}

sealed trait GkeAppConfig {
  def chartName: ChartName
  def chartVersion: ChartVersion
  def releaseNameSuffix: ReleaseNameSuffix
  def namespaceNameSuffix: NamespaceNameSuffix
  def serviceAccountName: ServiceAccountName
  def chart: Chart = Chart(chartName, chartVersion)
  def kubernetesServices: List[KubernetesService]
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
                                 minNumOfCpus: Int
) extends GkeAppConfig {
  override lazy val kubernetesServices: List[KubernetesService] = services.map(s => KubernetesService(ServiceId(-1), s))
}

final case class CromwellAppConfig(chartName: ChartName,
                                   chartVersion: ChartVersion,
                                   namespaceNameSuffix: NamespaceNameSuffix,
                                   releaseNameSuffix: ReleaseNameSuffix,
                                   services: List[ServiceConfig],
                                   serviceAccountName: ServiceAccountName,
                                   dbPassword: DbPassword
) extends GkeAppConfig {
  override lazy val kubernetesServices: List[KubernetesService] = services.map(s => KubernetesService(ServiceId(-1), s))
}

final case class CustomApplicationAllowListConfig(default: List[String], highSecurity: List[String])

final case class CustomAppConfig(chartName: ChartName,
                                 chartVersion: ChartVersion,
                                 releaseNameSuffix: ReleaseNameSuffix,
                                 namespaceNameSuffix: NamespaceNameSuffix,
                                 serviceAccountName: ServiceAccountName,
                                 enableCustomAppCheck: Boolean,
                                 customApplicationAllowList: CustomApplicationAllowListConfig
) extends GkeAppConfig {
  // Not known at config. Generated at runtime.
  override lazy val kubernetesServices: List[KubernetesService] = List.empty
}
