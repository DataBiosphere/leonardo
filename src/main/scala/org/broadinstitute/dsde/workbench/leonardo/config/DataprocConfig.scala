package org.broadinstitute.dsde.workbench.leonardo.config

case class DataprocConfig(serviceAccount: String,
                          dataprocDefaultZone: String,
                          dataprocDockerImage: String,
                          jupyterProxyDockerImage: String,
                          clusterFirewallRuleName: String,
                          configFolderPath: String,
                          initActionsScriptName: String,
                          clusterDockerComposeName: String,
                          serviceAccountPemName: String,
                          jupyterServerCrtName: String,
                          jupyterServerKeyName: String,
                          jupyterRootCaPemName: String,
                          jupyterRootCaKeyName: String,
                          jupyterProxySiteConfName: String,
                          clusterUrlBase: String,
                          jupyterServerName: String,
                          proxyServerName: String)