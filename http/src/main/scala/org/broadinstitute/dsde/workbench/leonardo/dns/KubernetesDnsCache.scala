package org.broadinstitute.dsde.workbench.leonardo.dns

import java.util.concurrent.TimeUnit

import cats.effect.{Blocker, ContextShift, Effect}
import com.google.common.cache.{CacheBuilder, CacheLoader, CacheStats}
import io.chrisdavenport.log4cats.Logger
import org.broadinstitute.dsde.workbench.google2.KubernetesSerializableName.ServiceName
import org.broadinstitute.dsde.workbench.leonardo.AppName
import org.broadinstitute.dsde.workbench.leonardo.config.{CacheConfig, ProxyConfig}
import org.broadinstitute.dsde.workbench.leonardo.dao.HostStatus
import org.broadinstitute.dsde.workbench.leonardo.db.DbReference
import org.broadinstitute.dsde.workbench.model.google.GoogleProject

import scala.concurrent.ExecutionContext

final case class KubernetesDnsCacheKey(googleProject: GoogleProject, appName: AppName, serviceName: ServiceName)

class KubernetesDnsCache[F[_]: Effect: ContextShift: Logger](proxyConfig: ProxyConfig,
                                                             dbRef: DbReference[F],
                                                             cacheConfig: CacheConfig,
                                                             blocker: Blocker)(implicit ec: ExecutionContext) {

  def getHostStatus(key: KubernetesDnsCacheKey): F[HostStatus] =
    blocker.blockOn(Effect[F].delay(hostStatusCache.get(key)))
  def size: Long = hostStatusCache.size
  def stats: CacheStats = hostStatusCache.stats

  private val hostStatusCache =
    CacheBuilder.newBuilder
      .expireAfterWrite(cacheConfig.cacheExpiryTime.toSeconds, TimeUnit.SECONDS)
      .maximumSize(cacheConfig.cacheMaxSize)
      .recordStats
      .build(
        new CacheLoader[KubernetesDnsCacheKey, HostStatus] {
          def load(key: KubernetesDnsCacheKey): HostStatus =
            // TODO query DB
            ???
        }
      )
}
