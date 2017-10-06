package org.broadinstitute.dsde.workbench.leonardo.config

case class ClusterResourcesConfig(
  configFolderPath: String,
  initActionsFileName: String,
  clusterDockerComposeName: String,
  leonardoServicePemName: String,
  jupyterServerCrtName: String,
  jupyterServerKeyName: String,
  jupyterRootCaPemName: String,
  jupyterRootCaKeyName: String,
  jupyterProxySiteConfName: String,
  jupyterInstallExtensionScript: String,
  userServiceAccountCredentials: String)