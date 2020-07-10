package org.broadinstitute.dsde.workbench.leonardo.config

import java.nio.file.Path

import org.broadinstitute.dsde.workbench.google2.KubernetesSerializableName.{SecretKey, SecretName}

case class IngressHelmConfig(secrets: List[SecretConfig])
case class SecretConfig(name: SecretName, secretFiles: List[SecretFile])
case class SecretFile(name: SecretKey, path: Path)
