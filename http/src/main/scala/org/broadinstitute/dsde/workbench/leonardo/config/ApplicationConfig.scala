package org.broadinstitute.dsde.workbench.leonardo
package config

import org.broadinstitute.dsde.workbench.model.WorkbenchEmail
import org.broadinstitute.dsde.workbench.model.google.GoogleProject

import java.net.URL
import java.nio.file.Path

case class ApplicationConfig(applicationName: String,
                             leoGoogleProject: GoogleProject,
                             leoServiceAccountJsonFile: Path,
                             leoServiceAccountEmail: WorkbenchEmail,
                             leoUrlBase: URL,
                             concurrency: Long,
                             azureHosting: Boolean
)
