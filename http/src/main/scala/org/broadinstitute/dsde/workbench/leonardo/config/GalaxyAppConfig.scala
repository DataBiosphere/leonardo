package org.broadinstitute.dsde.workbench.leonardo.config

import org.broadinstitute.dsde.workbench.google2.KubernetesSerializableName.ServiceAccountName
import org.broadinstitute.dsde.workbench.leonardo.{Chart, ServiceConfig}
import org.broadinstitute.dsp.{ChartName, ChartVersion}

final case class GalaxyAppConfig(releaseNameSuffix: String,
                                 chartName: ChartName,
                                 chartVersion: ChartVersion,
                                 namespaceNameSuffix: String,
                                 services: List[ServiceConfig],
                                 serviceAccount: ServiceAccountName,
                                 uninstallKeepHistory: Boolean,
                                 orchUrl: String,
                                 drsUrl: String) {

  def chart: Chart = Chart(chartName, chartVersion)
}
