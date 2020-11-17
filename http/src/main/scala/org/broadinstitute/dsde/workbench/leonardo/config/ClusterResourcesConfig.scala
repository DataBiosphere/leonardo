package org.broadinstitute.dsde.workbench.leonardo.config

import org.broadinstitute.dsde.workbench.leonardo.RuntimeResource

case class ClusterResourcesConfig(initActionsScript: RuntimeResource,
                                  gceInitScript: RuntimeResource,
                                  startupScript: RuntimeResource,
                                  shutdownScript: RuntimeResource,
                                  jupyterDockerCompose: RuntimeResource,
                                  jupyterDockerComposeGce: RuntimeResource,
                                  rstudioDockerCompose: RuntimeResource,
                                  proxyDockerCompose: RuntimeResource,
                                  welderDockerCompose: RuntimeResource,
                                  proxySiteConf: RuntimeResource,
                                  jupyterNotebookConfigUri: RuntimeResource,
                                  jupyterNotebookFrontendConfigUri: RuntimeResource,
                                  customEnvVarsConfigUri: RuntimeResource)

object ClusterResourcesConfig {
  val basePath = "init-resources"
}
