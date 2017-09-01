package org.broadinstitute.dsde.workbench.leonardo.config

case class DataprocConfig(serviceAccount: String,
                          dataprocDefaultZone: String,
                          dataprocDockerImage: String,
                          jupyterProxyDockerImage: String,
                          jupyterConfigFolderPath: String,
                          initActionsScriptName: String,
                          clusterDockerComposeName: String,
                          configFolderPath: String,
                          serviceAccountPemName: String,
                          jupyterServerCrtName: String,
                          jupyterServerKeyName: String,
                          jupyterRootCaPemName: String,
                          clusterUrlBase: String,
                          jupyterServerName: String,
                          proxyServerName: String)