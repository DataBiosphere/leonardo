package org.broadinstitute.dsde.workbench.leonardo.config

import org.broadinstitute.dsde.workbench.leonardo.model.GoogleServiceAccount
import org.broadinstitute.dsde.workbench.model.google.GoogleProject

case class DataprocConfig(
                           applicationName: String,
                           serviceAccount: GoogleServiceAccount,
                           dataprocDefaultRegion: String,
                           leoGoogleProject: GoogleProject,
                           dataprocDockerImage: String,
                           clusterUrlBase: String,
                           jupyterServerName: String,
                           createClusterAsPetServiceAccount: Boolean
                         )