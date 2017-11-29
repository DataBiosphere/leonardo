package org.broadinstitute.dsde.workbench.leonardo.config

import org.broadinstitute.dsde.workbench.model.WorkbenchEmail
import org.broadinstitute.dsde.workbench.model.google.GoogleProject

case class DataprocConfig(
                           applicationName: String,
                           serviceAccount: WorkbenchEmail,
                           dataprocDefaultRegion: String,
                           leoGoogleProject: GoogleProject,
                           dataprocDockerImage: String,
                           clusterUrlBase: String,
                           jupyterServerName: String,
                           createClusterAsPetServiceAccount: Boolean
                         )