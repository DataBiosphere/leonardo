package org.broadinstitute.dsde.workbench.leonardo.config

import org.broadinstitute.dsde.workbench.model.google.GoogleProject

import scala.concurrent.duration.FiniteDuration

case class DataprocConfig(
                           applicationName: String,
                           dataprocDefaultRegion: String,
                           dataprocZone: Option[String],
                           leoGoogleProject: GoogleProject,
                           dataprocDockerImage: String,
                           clusterUrlBase: String,
                           defaultExecutionTimeout: FiniteDuration,
                           jupyterServerName: String,
                           rstudioServerName: String,
                           welderServerName: String,
                           firewallRuleName: String,
                           networkTag: String,
                           defaultScopes: Set[String],
                           welderDockerImage: String,
                           vpcNetwork: Option[String],
                           vpcSubnet: Option[String],
                           projectVPCNetworkLabel: Option[String],
                           projectVPCSubnetLabel: Option[String]
                         )
