package org.broadinstitute.dsde.workbench.leonardo
package config
import org.broadinstitute.dsde.workbench.google2.{Location, RegionName}
import org.broadinstitute.dsde.workbench.leonardo.{CidrIP, KubernetesClusterVersion}

import scala.concurrent.duration.FiniteDuration

final case class AutopilotResource(cpuInMillicores: Int, memoryInGb: Int, ephemeralStorageInGb: Int)
final case class AutopilotConfig(welder: AutopilotResource, wonderShaper: AutopilotResource)
case class KubernetesClusterConfig(
  location: Location,
  region: RegionName,
  authorizedNetworks: List[CidrIP],
  version: KubernetesClusterVersion,
  nodepoolLockCacheExpiryTime: FiniteDuration,
  nodepoolLockCacheMaxSize: Int,
  autopilotConfig: AutopilotConfig
)
