package org.broadinstitute.dsde.workbench.leonardo.config

import org.broadinstitute.dsde.workbench.model.google.GoogleProject

final case class DataprocConfig(
  applicationName: String,
  dataprocDefaultRegion: String,
  dataprocZone: Option[String],
  leoGoogleProject: GoogleProject,
  jupyterImage: String,
  jupyterImageRegex: String,
  rstudioImageRegex: String,
  clusterUrlBase: String,
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
  projectVPCSubnetLabel: Option[String],
  welderEnabledNotebooksDir: String,
  welderDisabledNotebooksDir: String, // TODO: remove once welder is rolled out to all clusters
  legacyCustomDataprocImage: CustomDataprocImage,
  customDataprocImage: CustomDataprocImage,
  deployWelderLabel: Option[String],
  updateWelderLabel: Option[String],
  deployWelderCutoffDate: Option[String]
)

final case class CustomDataprocImage(asString: String) extends AnyVal
