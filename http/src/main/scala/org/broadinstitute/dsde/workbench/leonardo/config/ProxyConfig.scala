package org.broadinstitute.dsde.workbench.leonardo.config

import java.net.URL

import scala.concurrent.duration.FiniteDuration

case class ProxyConfig(proxyDomain: String,
                       proxyUrlBase: String,
                       proxyPort: Int,
                       dnsPollPeriod: FiniteDuration,
                       tokenCacheExpiryTime: FiniteDuration,
                       tokenCacheMaxSize: Int,
                       internalIdCacheExpiryTime: FiniteDuration,
                       internalIdCacheMaxSize: Int) {
  def getProxyServerHostName: String = {
    val url = new URL(proxyUrlBase)
    s"${url.getProtocol}://${url.getHost}" + (if (url.getPort == -1) "" else s":${url.getPort.toString}")
  }
}
