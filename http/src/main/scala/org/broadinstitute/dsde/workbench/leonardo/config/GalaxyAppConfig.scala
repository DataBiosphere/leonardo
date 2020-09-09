package org.broadinstitute.dsde.workbench.leonardo.config

import org.broadinstitute.dsde.workbench.leonardo.ServiceConfig
import org.broadinstitute.dsp.{ChartName, ChartVersion}

final case class GalaxyAppConfig(releaseNameSuffix: String,
                                 chartName: ChartName,
                                 chartVersion: ChartVersion,
                                 namespaceNameSuffix: String,
                                 services: List[ServiceConfig],
                                 serviceAccountSuffix: String,
                                 uninstallKeepHistory: Boolean) {

  def chartInfo: String = s"${chartName.asString}-${chartVersion.asString}"
}
