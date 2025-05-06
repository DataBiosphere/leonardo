package org.broadinstitute.dsde.workbench.leonardo
package config

case class ClusterResourcesConfig(
  initScript: RuntimeResource,
  legacyAOUInitScript: RuntimeResource, // AN-503: Delete once AOU has switched to using Dataproc 2.2.X in prod
  cloudInit: Option[RuntimeResource],
  startupScript: RuntimeResource,
  shutdownScript: RuntimeResource,
  jupyterDockerCompose: RuntimeResource,
  gpuDockerCompose: Option[RuntimeResource], // only applies to GCE runtimes
  rstudioDockerCompose: RuntimeResource,
  proxyDockerCompose: RuntimeResource,
  welderDockerCompose: RuntimeResource,
  proxySiteConf: RuntimeResource,
  jupyterNotebookConfigUri: RuntimeResource,
  jupyterNotebookFrontendConfigUri: RuntimeResource,
  customEnvVarsConfigUri: RuntimeResource
)

object ClusterResourcesConfig {
  val basePath = "init-resources"
}
