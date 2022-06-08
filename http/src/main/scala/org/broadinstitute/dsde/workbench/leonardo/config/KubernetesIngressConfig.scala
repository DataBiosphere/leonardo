package org.broadinstitute.dsde.workbench.leonardo
package config
import org.broadinstitute.dsde.workbench.google2.KubernetesSerializableName.{NamespaceName, ServiceName}
import org.broadinstitute.dsp.{ChartName, ChartVersion, Release}

final case class KubernetesIngressConfig(namespace: NamespaceName,
                                         release: Release,
                                         chartName: ChartName,
                                         chartVersion: ChartVersion,
                                         loadBalancerService: ServiceName,
                                         values: List[ValueConfig]
) {

  def chart: Chart = Chart(chartName, chartVersion)
}

final case class ValueConfig(value: String) extends AnyVal
