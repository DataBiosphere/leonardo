package org.broadinstitute.dsde.workbench.leonardo.config

import org.broadinstitute.dsde.workbench.leonardo.{ChartName, ServiceConfig}

case class GalaxyAppConfig(releaseNameSuffix: String,
                           chart: ChartName,
                           namespaceNameSuffix: String,
                           services: List[ServiceConfig],
                           serviceAccountSuffix: String,
                           uninstallKeepHistory: Boolean)
