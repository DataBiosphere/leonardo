package org.broadinstitute.dsde.workbench.leonardo.config

import org.broadinstitute.dsde.workbench.google.model.GoogleProject

case class DataprocConfig(
                           applicationName: String,
                           serviceAccount: String,
                           dataprocDefaultRegion: String,
                           leoGoogleProject: GoogleProject,
                           dataprocDockerImage: String,
                           clusterUrlBase: String,
                           jupyterServerName: String
                           )