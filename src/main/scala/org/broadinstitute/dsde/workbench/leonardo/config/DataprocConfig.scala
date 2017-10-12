package org.broadinstitute.dsde.workbench.leonardo.config

import org.broadinstitute.dsde.workbench.leonardo.model.{GoogleProject, GoogleServiceAccount}

case class DataprocConfig(
                           applicationName: String,
                           serviceAccount: GoogleServiceAccount,
                           dataprocDefaultRegion: String,
                           leoGoogleProject: GoogleProject,
                           dataprocDockerImage: String,
                           clusterUrlBase: String,
                           jupyterServerName: String
                           )