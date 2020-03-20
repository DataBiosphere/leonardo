package org.broadinstitute.dsde.workbench.leonardo.config

import org.broadinstitute.dsde.workbench.google2.{FirewallRuleName, NetworkName, RegionName, SubnetworkName}
import org.broadinstitute.dsde.workbench.leonardo.{IpRange, NetworkTag}

final case class VPCConfig(highSecurityProjectNetworkLabel: String,
                           highSecurityProjectSubnetworkLabel: String,
                           networkName: NetworkName,
                           subnetworkName: SubnetworkName,
                           subnetworkRegion: RegionName,
                           subnetworkIpRange: IpRange,
                           networkTag: NetworkTag,
                           httpsFirewallRule: FirewallRuleConfig,
                           sshFirewallRule: FirewallRuleConfig,
                           firewallsToRemove: List[FirewallRuleName])

final case class FirewallRuleConfig(name: FirewallRuleName,
                                    network: NetworkName,
                                    sourceRanges: List[IpRange],
                                    protocol: String,
                                    port: Int)
