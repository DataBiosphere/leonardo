package org.broadinstitute.dsde.workbench.leonardo.dao

import akka.util.Timeout
import org.broadinstitute.dsde.workbench.leonardo.dns.ClusterDnsCache.HostStatus
import org.broadinstitute.dsde.workbench.leonardo.dns.{ClusterDnsCache, DnsCacheKey}
import org.broadinstitute.dsde.workbench.leonardo.model.google.ClusterName
import org.broadinstitute.dsde.workbench.model.google.GoogleProject

import scala.concurrent.Future
import scala.concurrent.duration._

object Proxy {
  def getTargetHost(clusterDnsCache: ClusterDnsCache, googleProject: GoogleProject, clusterName: ClusterName): Future[HostStatus] = {
    implicit val timeout: Timeout = Timeout(5 seconds)
    clusterDnsCache.getHostStatus(DnsCacheKey(googleProject, clusterName)).mapTo[HostStatus]
  }
}
