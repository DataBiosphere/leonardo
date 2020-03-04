package org.broadinstitute.dsde.workbench.leonardo.config

import java.nio.file.Path

import org.broadinstitute.dsde.workbench.model.WorkbenchEmail
import org.broadinstitute.dsde.workbench.model.google.GoogleProject

case class ApplicationConfig(applicationName: String,
                             leoGoogleProject: GoogleProject,
                             leoServiceAccountJsonFile: Path,
                             leoServiceAccountEmail: WorkbenchEmail)
