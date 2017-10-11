package org.broadinstitute.dsde.workbench.leonardo.config

import java.io.File

case class ClusterResourcesConfig(
                                   initActionsFile: File,
                                   clusterDockerCompose: File,
                                   leonardoServicePem: File,
                                   jupyterServerCrt: File,
                                   jupyterServerKey: File,
                                   jupyterRootCaPem: File,
                                   jupyterRootCaKey: File,
                                   jupyterProxySiteConf: File,
                                   jupyterInstallExtensionScript: File,
                                   userServiceAccountCredentials: File
                                 )