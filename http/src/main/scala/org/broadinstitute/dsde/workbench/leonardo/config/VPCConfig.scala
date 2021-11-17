package org.broadinstitute.dsde.workbench.leonardo
package config
import org.broadinstitute.dsde.workbench.google2.{FirewallRuleName, NetworkName, RegionName, SubnetworkName}
import org.broadinstitute.dsde.workbench.leonardo.{IpRange, NetworkTag}

import scala.concurrent.duration.FiniteDuration

final case class VPCConfig(highSecurityProjectNetworkLabel: NetworkLabel,
                           highSecurityProjectSubnetworkLabel: SubnetworkLabel,
                           networkName: NetworkName,
                           networkTag: NetworkTag,
                           privateAccessNetworkTag: NetworkTag,
                           autoCreateSubnetworks: Boolean,
                           subnetworkName: SubnetworkName,
                           subnetworkRegionIpRangeMap: Map[RegionName, IpRange],
                           firewallsToAdd: List[FirewallRuleConfig],
                           firewallsToRemove: List[FirewallRuleName],
                           pollPeriod: FiniteDuration,
                           maxAttempts: Int)

final case class FirewallRuleConfig(namePrefix: String,
                                    sourceRanges: Map[RegionName, List[IpRange]],
                                    allowed: List[Allowed])

final case class Allowed(protocol: String, port: Option[String])

final case class NetworkLabel(value: String) extends AnyVal
final case class SubnetworkLabel(value: String) extends AnyVal
