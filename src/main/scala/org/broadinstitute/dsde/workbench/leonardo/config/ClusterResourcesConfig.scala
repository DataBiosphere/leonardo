package org.broadinstitute.dsde.workbench.leonardo.config

case class ClusterResourcesConfig(
                                   configFolderPath: String,
                                   initActionsScript: String,
                                   clusterDockerCompose: String,
                                   leonardoServicePem: String,
                                   jupyterServerCrt: String,
                                   jupyterServerKey: String,
                                   jupyterRootCaPem: String,
                                   jupyterRootCaKey: String,
                                   jupyterProxySiteConf: String,
                                   jupyterInstallExtensionScript: String,
                                   userServiceAccountCredentials: String
                                 )