package org.broadinstitute.dsde.workbench.leonardo.config

import org.broadinstitute.dsde.workbench.leonardo.model.ClusterResource

case class ClusterResourcesConfig(initActionsScript: ClusterResource,
                                  jupyterDockerCompose: ClusterResource,
                                  rstudioDockerCompose: ClusterResource,
                                  proxyDockerCompose: ClusterResource,
                                  proxySiteConf: ClusterResource,
                                  googleSignInJs: ClusterResource,
                                  jupyterGooglePlugin: ClusterResource,
                                  jupyterLabGooglePlugin: ClusterResource,
                                  jupyterNotebookConfigUri: ClusterResource
                                 )

object ClusterResourcesConfig {
  val basePath = "jupyter"
}