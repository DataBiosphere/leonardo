package org.broadinstitute.dsde.workbench.leonardo.config

import org.broadinstitute.dsde.workbench.model.google.{GoogleProject, ServiceAccountName}

case class DataprocConfig(
                           applicationName: String,
                           serviceAccount: ServiceAccountName,
                           dataprocDefaultRegion: String,
                           leoGoogleProject: GoogleProject,
                           dataprocDockerImage: String,
                           clusterUrlBase: String,
                           jupyterServerName: String,
                           createClusterAsPetServiceAccount: Boolean
                         )