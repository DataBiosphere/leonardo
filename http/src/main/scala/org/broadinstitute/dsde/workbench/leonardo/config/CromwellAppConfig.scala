package org.broadinstitute.dsde.workbench.leonardo.config

import org.broadinstitute.dsde.workbench.google2.KubernetesSerializableName.ServiceAccountName
import org.broadinstitute.dsde.workbench.leonardo.{Chart, ServiceConfig}
import org.broadinstitute.dsp.{ChartName, ChartVersion}

final case class CromwellAppConfig(chartName: ChartName,
                                   chartVersion: ChartVersion,
                                   namespaceNameSuffix: String,
                                   releaseNameSuffix: String,
                                   services: List[ServiceConfig],
                                   serviceAccountName: ServiceAccountName,
                                   dbPassword: String
                                  ) {

  def chart: Chart = Chart(chartName, chartVersion)
}
