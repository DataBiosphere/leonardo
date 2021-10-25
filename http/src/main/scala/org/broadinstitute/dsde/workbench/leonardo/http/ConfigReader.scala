package org.broadinstitute.dsde.workbench.leonardo
package http

import pureconfig.ConfigSource
import _root_.pureconfig.generic.auto._
import org.broadinstitute.dsde.workbench.leonardo.ConfigImplicits._
import org.broadinstitute.dsde.workbench.leonardo.util.TerraAppSetupChartConfig
import org.broadinstitute.dsde.workbench.leonardo.config.PersistentDiskConfig

object ConfigReader {
  lazy val appConfig =
    ConfigSource
      .fromConfig(org.broadinstitute.dsde.workbench.leonardo.config.Config.config)
      .loadOrThrow[AppConfig]
}

// Note: pureconfig supports reading kebab case into camel case in code by default
// More docs see https://pureconfig.github.io/docs/index.html
final case class AppConfig(
  terraAppSetupChart: TerraAppSetupChartConfig,
  persistentDisk: PersistentDiskConfig
)
