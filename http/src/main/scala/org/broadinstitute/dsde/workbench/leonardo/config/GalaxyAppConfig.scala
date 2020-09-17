package org.broadinstitute.dsde.workbench.leonardo.config

import org.broadinstitute.dsde.workbench.leonardo.{Chart, KubernetesServiceAccount, ServiceConfig}
import org.broadinstitute.dsp.{ChartName, ChartVersion}

final case class GalaxyAppConfig(releaseNameSuffix: String,
                                 chartName: ChartName,
                                 chartVersion: ChartVersion,
                                 namespaceNameSuffix: String,
                                 services: List[ServiceConfig],
                                 serviceAccount: KubernetesServiceAccount,
                                 uninstallKeepHistory: Boolean) {

  def chart: Chart = Chart(chartName, chartVersion)
}
