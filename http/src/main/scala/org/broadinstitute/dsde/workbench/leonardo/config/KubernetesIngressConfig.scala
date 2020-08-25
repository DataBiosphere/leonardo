package org.broadinstitute.dsde.workbench.leonardo.config

import java.nio.file.Path

import org.broadinstitute.dsde.workbench.google2.KubernetesSerializableName.{
  NamespaceName,
  SecretKey,
  SecretName,
  ServiceName
}
import org.broadinstitute.dsde.workbench.leonardo.{ChartName, ReleaseName}

final case class KubernetesIngressConfig(namespace: NamespaceName,
                                         release: ReleaseName,
                                         chart: ChartName,
                                         loadBalancerService: ServiceName,
                                         values: List[ValueConfig],
                                         secrets: List[SecretConfig])

final case class ValueConfig(value: String) extends AnyVal
final case class SecretConfig(name: SecretName, secretFiles: List[SecretFile])
final case class SecretFile(name: SecretKey, path: Path)
