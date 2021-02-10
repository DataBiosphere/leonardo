package org.broadinstitute.dsde.workbench.leonardo
package http

import pureconfig.ConfigSource
import _root_.pureconfig.generic.auto._
import org.broadinstitute.dsde.workbench.leonardo.ConfigImplicits._
import org.broadinstitute.dsde.workbench.leonardo.util.TerraAppSetupChartConfig

object ConfigReader {
  val appConfig = ConfigSource
    .fromConfig(org.broadinstitute.dsde.workbench.leonardo.config.Config.config)
    .loadOrThrow[AppConfig]
}

final case class AppConfig(
  terraAppSetupChart: TerraAppSetupChartConfig
)
