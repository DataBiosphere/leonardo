package org.broadinstitute.dsde.workbench.leonardo

import akka.http.scaladsl.model.Uri.Host

package object algebra {

  // This hostname is used by the ProxyService and also needs to be specified in the Galaxy ingress resource
  def kubernetesProxyHost(cluster: KubernetesCluster, proxyDomain: String): Host = {
    val prefix = Math.abs(cluster.getGkeClusterId.toString.hashCode).toString
    Host(prefix + proxyDomain)
  }
}
