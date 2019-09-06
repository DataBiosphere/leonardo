package org.broadinstitute.dsde.workbench.leonardo.config


import scala.concurrent.duration.FiniteDuration

case class ProxyConfig(
                        jupyterProxyDockerImage: String,
                        proxyServerName: String,
                        jupyterPort: Int,
                        jupyterProtocol: String,
                        jupyterDomain: String,
                        dnsPollPeriod: FiniteDuration,
                        cacheExpiryTime: FiniteDuration,
                        cacheMaxSize: Int
                      )
