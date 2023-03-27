package org.broadinstitute.dsde.workbench.leonardo
package config

import java.net.URL
import java.nio.file.Path

import org.broadinstitute.dsde.workbench.model.WorkbenchEmail
import org.broadinstitute.dsde.workbench.model.google.GoogleProject

case class ApplicationConfig(applicationName: String,
                             leoGoogleProject: GoogleProject,
                             leoServiceAccountJsonFile: Path,
                             leoServiceAccountEmail: WorkbenchEmail,
                             leoUrlBase: String,
                             concurrency: Long
) {
  def getLeoUrl: String = {
    val url = new URL(leoUrlBase)

    // TODO is this needed? Copied from ProxyConfig
    // The port is specified in fiabs, but generally unset otherwise
    val portStr = if (url.getPort == -1) "" else s":${url.getPort.toString}"

    s"${url.getProtocol}://${url.getHost}${portStr}"
  }
}
