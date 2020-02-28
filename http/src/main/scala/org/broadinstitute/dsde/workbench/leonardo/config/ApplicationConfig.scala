package org.broadinstitute.dsde.workbench.leonardo.config

import java.io.File

import org.broadinstitute.dsde.workbench.model.WorkbenchEmail
import org.broadinstitute.dsde.workbench.model.google.GoogleProject

case class ApplicationConfig(applicationName: String,
                             leoGoogleProject: GoogleProject,
                             leoServiceAccountJsonFile: File,
                             leoServiceAccountEmail: WorkbenchEmail)
