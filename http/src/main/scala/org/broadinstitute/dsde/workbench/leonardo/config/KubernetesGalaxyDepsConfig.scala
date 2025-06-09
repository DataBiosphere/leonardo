package org.broadinstitute.dsde.workbench.leonardo.config

import org.broadinstitute.dsde.workbench.google2.KubernetesSerializableName.NamespaceName
import org.broadinstitute.dsde.workbench.leonardo.Chart
import org.broadinstitute.dsp.{ChartName, ChartVersion, Release}

final case class KubernetesGalaxyDepsConfig(namespace: NamespaceName,
                                             release: Release,
                                             chartName: ChartName,
                                             chartVersion: ChartVersion,
                                            values: List[ValueConfig]) {
  def chart: Chart = Chart(chartName, chartVersion)
}
