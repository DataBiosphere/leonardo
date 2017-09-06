package org.broadinstitute.dsde.workbench.leonardo.config

case class DataprocConfig(serviceAccount: String,
                          dataprocInitScriptURI: String,
                          dataprocDefaultRegion: String,
                          dataprocDockerImage: String,
                          serviceAccountPemPath: String,
                          clusterUrlBase: String)