package org.broadinstitute.dsde.workbench.leonardo.config

import scala.concurrent.duration.FiniteDuration

case class ProxyConfig(
  jupyterProxyDockerImage: String,
  proxyServerName: String,
  jupyterPort: Int,
  jupyterProtocol: String,
  jupyterDomain: String,
  dnsPollPeriod: FiniteDuration,
  tokenCacheExpiryTime: FiniteDuration,
  tokenCacheMaxSize: Int,
  internalIdCacheExpiryTime: FiniteDuration,
  internalIdCacheMaxSize: Int
)
