package org.broadinstitute.dsde.workbench.leonardo
package config

import java.net.URL
import java.nio.file.Path

import org.broadinstitute.dsde.workbench.model.WorkbenchEmail
import org.broadinstitute.dsde.workbench.model.google.GoogleProject

case class ApplicationConfig(applicationName: String,
                             leoGoogleProject: GoogleProject,
                             leoServiceAccountJsonFile: Option[
                               Path
                             ], // If this is defined, we're running in GCP mode; otherwise, we're running in Azure mode
                             leoServiceAccountEmail: WorkbenchEmail,
                             leoUrlBase: URL,
                             concurrency: Long
)
