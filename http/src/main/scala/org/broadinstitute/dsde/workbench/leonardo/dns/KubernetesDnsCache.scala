package org.broadinstitute.dsde.workbench.leonardo.dns

import java.nio.charset.StandardCharsets
import java.util.Base64
import java.util.concurrent.TimeUnit

import akka.http.scaladsl.model.Uri.Host
import cats.effect.implicits._
import cats.effect.{Blocker, ContextShift, Effect}
import cats.implicits._
import com.google.common.cache.{CacheBuilder, CacheLoader, CacheStats}
import org.broadinstitute.dsde.workbench.leonardo.config.{CacheConfig, ProxyConfig}
import org.broadinstitute.dsde.workbench.leonardo.dao.HostStatus
import org.broadinstitute.dsde.workbench.leonardo.dao.HostStatus.{HostNotFound, HostNotReady, HostReady}
import org.broadinstitute.dsde.workbench.leonardo.db.{DbReference, GetAppResult, KubernetesServiceDbQueries}
import org.broadinstitute.dsde.workbench.leonardo.{AppName, KubernetesCluster}
import org.broadinstitute.dsde.workbench.model.google.GoogleProject

import scala.concurrent.ExecutionContext

final case class KubernetesDnsCacheKey(googleProject: GoogleProject, appName: AppName)

class KubernetesDnsCache[F[_]: Effect: ContextShift](proxyConfig: ProxyConfig,
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
          def load(key: KubernetesDnsCacheKey): HostStatus = {
            val res = for {
              appResultOpt <- dbRef.inTransaction {
                KubernetesServiceDbQueries.getActiveFullAppByName(key.googleProject, key.appName)
              }
              hostStatus <- appResultOpt match {
                case None            => Effect[F].pure[HostStatus](HostNotFound)
                case Some(appResult) => hostStatusByAppResult(appResult)
              }
            } yield hostStatus

            res.toIO.unsafeRunSync
          }
        }
      )

  private def host(cluster: KubernetesCluster): Host = {
    val prefix =
      Base64.getUrlEncoder().encodeToString(cluster.getGkeClusterId.toString.getBytes(StandardCharsets.UTF_8))
    Host(prefix + proxyConfig.proxyDomain)
  }

  private def hostStatusByAppResult(appResult: GetAppResult): F[HostStatus] =
    appResult.cluster.asyncFields.map(_.externalIp) match {
      case None => Effect[F].pure(HostNotReady)
      case Some(ip) =>
        val h = host(appResult.cluster)
        HostToIpMapping.hostToIpMapping.getAndUpdate(_ + (h -> ip)).as[HostStatus](HostReady(h)).to[F]
    }
}
