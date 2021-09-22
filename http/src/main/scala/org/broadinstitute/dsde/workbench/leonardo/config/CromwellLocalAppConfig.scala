package org.broadinstitute.dsde.workbench.leonardo.config

import org.broadinstitute.dsde.workbench.google2.KubernetesSerializableName.ServiceAccountName
import org.broadinstitute.dsde.workbench.leonardo.{Chart, ServiceConfig}
import org.broadinstitute.dsp.{ChartName, ChartVersion}

final case class CromwellLocalAppConfig(chartName: ChartName,
                                        chartVersion: ChartVersion,
                                        namespaceNameSuffix: String,
                                        services: List[ServiceConfig],
                                        serviceAccountName: ServiceAccountName) {

  def chart: Chart = Chart(chartName, chartVersion)
}
