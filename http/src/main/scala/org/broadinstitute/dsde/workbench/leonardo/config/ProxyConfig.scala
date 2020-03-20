package org.broadinstitute.dsde.workbench.leonardo.config

import scala.concurrent.duration.FiniteDuration

case class ProxyConfig(proxyDomain: String,
                       proxyUrlBase: String,
                       proxyPort: Int,
                       dnsPollPeriod: FiniteDuration,
                       tokenCacheExpiryTime: FiniteDuration,
                       tokenCacheMaxSize: Int,
                       internalIdCacheExpiryTime: FiniteDuration,
                       internalIdCacheMaxSize: Int)
