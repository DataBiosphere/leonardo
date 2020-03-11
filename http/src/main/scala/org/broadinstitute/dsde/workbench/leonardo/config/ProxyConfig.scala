package org.broadinstitute.dsde.workbench.leonardo.config

import org.broadinstitute.dsde.workbench.google2.RegionName

import scala.concurrent.duration.FiniteDuration

case class ProxyConfig(proxyDomain: String,
                       proxyUrlBase: String,
                       proxyPort: Int,
                       proxyProtocol: String,
                       firewallRuleName: String,
                       networkTag: String,
                       projectVPCNetworkLabel: String,
                       projectVPCSubnetLabel: String,
                       projectVPCSubnetRegion: RegionName,
                       dnsPollPeriod: FiniteDuration,
                       tokenCacheExpiryTime: FiniteDuration,
                       tokenCacheMaxSize: Int,
                       internalIdCacheExpiryTime: FiniteDuration,
                       internalIdCacheMaxSize: Int)
