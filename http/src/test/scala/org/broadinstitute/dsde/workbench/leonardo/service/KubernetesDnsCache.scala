package org.broadinstitute.dsde.workbench.leonardo.service

import cats.effect.{Blocker, ContextShift, Effect}
import com.google.common.cache.CacheStats
import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.dsde.workbench.leonardo.db.DbReference
import org.broadinstitute.dsde.workbench.leonardo.dns.ClusterDnsCache.HostStatus
import org.broadinstitute.dsde.workbench.model.google.GoogleProject

import scala.concurrent.ExecutionContext

class KubernetesDnsCache[F[_]: Effect: ContextShift](kubernetesProxyConfig: _,
                                                  blocker: Blocker)(implicit ec: ExecutionContext, dbReference: DbReference[F])
  extends LazyLogging {

  case class DnsCacheKey(googleProject: GoogleProject, AppName: _)

//  def getHostStatus(key: DnsCacheKey): F[HostStatus] = ???
//    blocker.blockOn(Effect[F].delay(projectClusterToHostStatus.get(key)))
//  def size: Long = projectClusterToHostStatus.size
//  def stats: CacheStats = projectAppToHostStatus.stats
}
