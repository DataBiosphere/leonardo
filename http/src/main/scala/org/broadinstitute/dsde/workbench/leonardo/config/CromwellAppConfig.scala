package org.broadinstitute.dsde.workbench.leonardo.config

import org.broadinstitute.dsde.workbench.google2.KubernetesSerializableName.ServiceAccountName
import org.broadinstitute.dsde.workbench.leonardo.{
  Chart,
  DbPassword,
  NamespaceNameSuffix,
  ReleaseNameSuffix,
  ServiceConfig
}
import org.broadinstitute.dsp.{ChartName, ChartVersion}

final case class CromwellAppConfig(chartName: ChartName,
                                   chartVersion: ChartVersion,
                                   namespaceNameSuffix: NamespaceNameSuffix,
                                   releaseNameSuffix: ReleaseNameSuffix,
                                   services: List[ServiceConfig],
                                   serviceAccountName: ServiceAccountName,
                                   dbPassword: DbPassword) {

  def chart: Chart = Chart(chartName, chartVersion)
}
