package org.broadinstitute.dsde.workbench.leonardo.config

import org.broadinstitute.dsde.workbench.google2.KubernetesSerializableName.ServiceAccountName
import org.broadinstitute.dsde.workbench.leonardo.{Chart, KubernetesService, ServiceConfig, ServiceId}
import org.broadinstitute.dsp.{ChartName, ChartVersion}

sealed trait GkeAppConfig {
  def chartName: ChartName
  def chartVersion: ChartVersion
  def releaseNameSuffix: String
  def namespaceNameSuffix: String

  def chart: Chart = Chart(chartName, chartVersion)
  def kubernetesServices: List[KubernetesService]
}

final case class GalaxyAppConfig(releaseNameSuffix: String,
                                 chartName: ChartName,
                                 chartVersion: ChartVersion,
                                 namespaceNameSuffix: String,
                                 services: List[ServiceConfig],
                                 serviceAccount: ServiceAccountName,
                                 uninstallKeepHistory: Boolean,
                                 postgresPassword: String,
                                 orchUrl: String,
                                 drsUrl: String) extends GkeAppConfig {
  override lazy val kubernetesServices: List[KubernetesService] = services.map(s => KubernetesService(ServiceId(-1), s))
}

final case class CromwellAppConfig(chartName: ChartName,
                                   chartVersion: ChartVersion,
                                   namespaceNameSuffix: String,
                                   releaseNameSuffix: String,
                                   services: List[ServiceConfig],
                                   serviceAccountName: ServiceAccountName) extends GkeAppConfig {
  override lazy val kubernetesServices: List[KubernetesService] = services.map(s => KubernetesService(ServiceId(-1), s))
}

final case class CustomAppConfig(chartName: ChartName,
                                 chartVersion: ChartVersion,
                                 releaseNameSuffix: String,
                                 namespaceNameSuffix: String) extends GkeAppConfig {
  // Not known at config. Generated at runtime.
  override lazy val kubernetesServices: List[KubernetesService] = List.empty
}
