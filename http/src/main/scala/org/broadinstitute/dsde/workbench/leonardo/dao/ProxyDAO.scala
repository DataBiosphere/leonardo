package org.broadinstitute.dsde.workbench.leonardo
package dao

import cats.effect.implicits._
import cats.effect.{Concurrent, ContextShift, Timer}
import org.broadinstitute.dsde.workbench.leonardo.dns.ClusterDnsCache.HostStatus
import org.broadinstitute.dsde.workbench.leonardo.dns.{ClusterDnsCache, DnsCacheKey}
import org.broadinstitute.dsde.workbench.model.google.GoogleProject

import scala.concurrent.duration._

object Proxy {
  def getTargetHost[F[_]: Timer: ContextShift: Concurrent](clusterDnsCache: ClusterDnsCache[F],
                                                           googleProject: GoogleProject,
                                                           runtimeName: RuntimeName): F[HostStatus] =
    clusterDnsCache.getHostStatus(DnsCacheKey(googleProject, runtimeName)).timeout(5 seconds)
}
