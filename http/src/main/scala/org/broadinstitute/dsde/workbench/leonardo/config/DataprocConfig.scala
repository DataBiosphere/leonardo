package org.broadinstitute.dsde.workbench.leonardo
package config

import org.broadinstitute.dsde.workbench.google2.RegionName
import org.broadinstitute.dsde.workbench.leonardo.CustomImage.DataprocCustomImage

final case class DataprocConfig(
  defaultScopes: Set[String],
  customDataprocImage: DataprocCustomImage,
  sparkMemoryConfigRatio: Option[Double],
  minimumRuntimeMemoryInGb: Option[Double],
  runtimeConfigDefaults: RuntimeConfig.DataprocConfig,
  supportedRegions: Set[RegionName]
)
