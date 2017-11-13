package org.broadinstitute.dsde.workbench.leonardo.config

import org.broadinstitute.dsde.workbench.leonardo.model.ClusterResource

case class ClusterResourcesConfig(initActionsScript: ClusterResource,
                                  clusterDockerCompose: ClusterResource,
                                  jupyterProxySiteConf: ClusterResource,
                                  jupyterInstallExtensionScript: ClusterResource,
                                  jupyterCustomJs: ClusterResource,
                                  jupyterGoogleSignInJs: ClusterResource)

object ClusterResourcesConfig {
  val basePath = "jupyter"
}