package org.broadinstitute.dsde.workbench.leonardo
package dao

import cats.effect.implicits._
import cats.effect.{Concurrent, ContextShift, Timer}
import org.broadinstitute.dsde.workbench.leonardo.dns.ClusterDnsCache.HostStatus
import org.broadinstitute.dsde.workbench.leonardo.dns.{ClusterDnsCache, DnsCacheKey}
import org.broadinstitute.dsde.workbench.leonardo.model.google.ClusterName
import org.broadinstitute.dsde.workbench.model.google.GoogleProject

import scala.concurrent.duration._

object Proxy {
  def getTargetHost[F[_]: Timer: ContextShift: Concurrent](clusterDnsCache: ClusterDnsCache[F],
                                                           googleProject: GoogleProject,
                                                           clusterName: ClusterName): F[HostStatus] =
    clusterDnsCache.getHostStatus(DnsCacheKey(googleProject, clusterName)).timeout(5 seconds)
}
