package org.broadinstitute.dsde.workbench.leonardo.config

import org.broadinstitute.dsde.workbench.model.google.GoogleProject

case class DataprocConfig(
                           applicationName: String,
                           dataprocDefaultRegion: String,
                           leoGoogleProject: GoogleProject,
                           dataprocDockerImage: String,
                           clusterUrlBase: String,
                           jupyterServerName: String,
                           firewallRuleName: String,
                           networkTag: String,
                           vpcNetwork: Option[String],
                           vpcSubnet: Option[String]
                         )
