package org.broadinstitute.dsde.workbench.leonardo
package config
import java.net.URL

import scala.concurrent.duration.FiniteDuration

case class ProxyConfig(proxyDomain: String,
                       proxyUrlBase: String,
                       proxyPort: Int,
                       dnsPollPeriod: FiniteDuration,
                       tokenCacheExpiryTime: FiniteDuration,
                       tokenCacheMaxSize: Int,
                       internalIdCacheExpiryTime: FiniteDuration,
                       internalIdCacheMaxSize: Int,
                       isProxyCookiePartitioned: Boolean
) {
  def getProxyServerHostName: String = {
    val url = new URL(proxyUrlBase)
    // The port is specified in fiabs, but generally unset otherwise
    val portStr = if (url.getPort == -1) "" else s":${url.getPort.toString}"
    s"${url.getProtocol}://${url.getHost}${portStr}"
  }
}
