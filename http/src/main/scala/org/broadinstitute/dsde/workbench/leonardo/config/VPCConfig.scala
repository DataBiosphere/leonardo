package org.broadinstitute.dsde.workbench.leonardo.config

import org.broadinstitute.dsde.workbench.google2.{FirewallRuleName, NetworkName, RegionName, SubnetworkName}
import org.broadinstitute.dsde.workbench.leonardo.{IpRange, NetworkTag}

import scala.concurrent.duration.FiniteDuration

final case class VPCConfig(highSecurityProjectNetworkLabel: String,
                           highSecurityProjectSubnetworkLabel: String,
                           networkName: NetworkName,
                           networkTag: NetworkTag,
                           autoCreateSubnetworks: Boolean,
                           subnetworkName: SubnetworkName,
                           subnetworkRegion: RegionName,
                           subnetworkIpRange: IpRange,
                           httpsFirewallRule: FirewallRuleConfig,
                           sshFirewallRule: FirewallRuleConfig,
                           firewallsToRemove: List[FirewallRuleName],
                           pollPeriod: FiniteDuration,
                           maxAttempts: Int)

final case class FirewallRuleConfig(name: FirewallRuleName,
                                    network: NetworkName,
                                    sourceRanges: List[IpRange],
                                    protocol: String,
                                    port: Int)
