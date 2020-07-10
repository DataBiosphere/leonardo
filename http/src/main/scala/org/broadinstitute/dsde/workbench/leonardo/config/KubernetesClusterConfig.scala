package org.broadinstitute.dsde.workbench.leonardo.config

import org.broadinstitute.dsde.workbench.google2.{Location, RegionName}
import org.broadinstitute.dsde.workbench.leonardo.CidrIP

case class KubernetesClusterConfig(
  location: Location,
  region: RegionName,
  authorizedNetworks: List[CidrIP]
)
