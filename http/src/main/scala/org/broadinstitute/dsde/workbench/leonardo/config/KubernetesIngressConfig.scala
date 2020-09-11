package org.broadinstitute.dsde.workbench.leonardo.config

import java.nio.file.Path

import org.broadinstitute.dsde.workbench.google2.KubernetesSerializableName.{
  NamespaceName,
  SecretKey,
  SecretName,
  ServiceName
}
import org.broadinstitute.dsp.{ChartName, ChartVersion, Release}

final case class KubernetesIngressConfig(namespace: NamespaceName,
                                         release: Release,
                                         chartName: ChartName,
                                         chartVersion: ChartVersion,
                                         loadBalancerService: ServiceName,
                                         values: List[ValueConfig],
                                         secrets: List[SecretConfig]) {

  def chartInfo: String = s"${chartName.asString}-${chartVersion.asString}"
}

final case class ValueConfig(value: String) extends AnyVal
final case class SecretConfig(name: SecretName, secretFiles: List[SecretFile])
final case class SecretFile(name: SecretKey, path: Path)
