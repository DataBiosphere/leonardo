package org.broadinstitute.dsde.workbench.leonardo

package config
import org.broadinstitute.dsde.workbench.google2.KubernetesSerializableName.ServiceAccountName
import org.broadinstitute.dsp.{ChartName, ChartVersion}

final case class GalaxyAppConfig(releaseNameSuffix: String,
                                 chartName: ChartName,
                                 chartVersion: ChartVersion,
                                 namespaceNameSuffix: String,
                                 services: List[ServiceConfig],
                                 serviceAccount: ServiceAccountName,
                                 uninstallKeepHistory: Boolean,
                                 postgresPassword: String,
                                 orchUrl: String,
                                 drsUrl: String) {

  def chart: Chart = Chart(chartName, chartVersion)
}
