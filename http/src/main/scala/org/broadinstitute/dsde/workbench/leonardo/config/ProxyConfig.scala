package org.broadinstitute.dsde.workbench.leonardo.config

import scala.concurrent.duration.FiniteDuration

case class ProxyConfig(proxyDomain: String,
                       proxyUrlBase: String,
                       proxyPort: Int,
                       proxyProtocol: String,
                       firewallRuleName: String,
                       networkTag: String,
                       projectVPCNetworkLabel: String,
                       projectVPCSubnetLabel: String,
                       dnsPollPeriod: FiniteDuration,
                       tokenCacheExpiryTime: FiniteDuration,
                       tokenCacheMaxSize: Int,
                       internalIdCacheExpiryTime: FiniteDuration,
                       internalIdCacheMaxSize: Int)
