package org.broadinstitute.dsde.workbench.leonardo

package config
import org.broadinstitute.dsde.workbench.google2.KubernetesSerializableName.ServiceAccountName
import org.broadinstitute.dsp.{ChartName, ChartVersion}

final case class GalaxyAppConfig(releaseNameSuffix: ReleaseNameSuffix,
                                 chartName: ChartName,
                                 chartVersion: ChartVersion,
                                 namespaceNameSuffix: NamespaceNameSuffix,
                                 services: List[ServiceConfig],
                                 serviceAccount: ServiceAccountName,
                                 uninstallKeepHistory: Boolean,
                                 postgresPassword: DbPassword,
                                 orchUrl: GalaxyOrchUrl,
                                 drsUrl: GalaxyDrsUrl) {

  def chart: Chart = Chart(chartName, chartVersion)
}
