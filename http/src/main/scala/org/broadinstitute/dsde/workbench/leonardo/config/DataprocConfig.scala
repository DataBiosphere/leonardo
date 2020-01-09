package org.broadinstitute.dsde.workbench.leonardo.config

import org.broadinstitute.dsde.workbench.leonardo.model.MemorySize
import org.broadinstitute.dsde.workbench.model.google.GoogleProject

final case class DataprocConfig(
  applicationName: String,
  dataprocDefaultRegion: String,
  dataprocZone: Option[String],
  leoGoogleProject: GoogleProject,
  clusterUrlBase: String,
  jupyterServerName: String,
  rstudioServerName: String,
  welderServerName: String,
  firewallRuleName: String,
  networkTag: String,
  defaultScopes: Set[String],
  vpcNetwork: Option[String],
  vpcSubnet: Option[String],
  projectVPCNetworkLabel: Option[String],
  projectVPCSubnetLabel: Option[String],
  legacyCustomDataprocImage: CustomDataprocImage,
  customDataprocImage: CustomDataprocImage,
  dataprocReservedMemory: Option[MemorySize]
)

final case class CustomDataprocImage(asString: String) extends AnyVal
