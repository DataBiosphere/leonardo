package org.broadinstitute.dsde.workbench.leonardo.config

case class DataprocConfig(
                          applicationName: String,
                          serviceAccount: String,
                          dataprocDefaultRegion: String,
                          leoGoogleProject: String,
                          dataprocDockerImage: String,
                          jupyterProxyDockerImage: String,
                          includeDeletedKey: String,
                          clusterUrlBase: String,
                          jupyterServerName: String,
                          proxyServerName: String
                         )