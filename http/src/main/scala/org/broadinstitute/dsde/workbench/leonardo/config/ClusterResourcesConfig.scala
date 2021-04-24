package org.broadinstitute.dsde.workbench.leonardo
package config

case class ClusterResourcesConfig(initScript: RuntimeResource,
                                  startupScript: RuntimeResource,
                                  shutdownScript: RuntimeResource,
                                  jupyterDockerCompose: RuntimeResource,
                                  gpuDockerCompose: Option[RuntimeResource], //only applies to GCE runtimes
                                  rstudioDockerCompose: RuntimeResource,
                                  proxyDockerCompose: RuntimeResource,
                                  welderDockerCompose: RuntimeResource,
                                  cryptoDetectorDockerCompose: RuntimeResource,
                                  proxySiteConf: RuntimeResource,
                                  jupyterNotebookConfigUri: RuntimeResource,
                                  jupyterNotebookFrontendConfigUri: RuntimeResource,
                                  customEnvVarsConfigUri: RuntimeResource)

object ClusterResourcesConfig {
  val basePath = "init-resources"
}
