package org.broadinstitute.dsde.workbench.leonardo.dns

import akka.http.scaladsl.model.Uri.Host
import cats.effect.concurrent.Ref
import cats.effect.implicits._
import cats.effect.{Blocker, ContextShift, Effect, Timer}
import cats.syntax.all._
import com.google.common.cache.{CacheBuilder, CacheLoader, CacheStats}
import fs2.Stream
import org.typelevel.log4cats.Logger
import org.broadinstitute.dsde.workbench.leonardo.AppName
import org.broadinstitute.dsde.workbench.leonardo.config.{CacheConfig, ProxyConfig}
import org.broadinstitute.dsde.workbench.leonardo.dao.HostStatus
import org.broadinstitute.dsde.workbench.leonardo.dao.HostStatus.{HostNotFound, HostNotReady, HostReady}
import org.broadinstitute.dsde.workbench.leonardo.db.{DbReference, KubernetesServiceDbQueries}
import org.broadinstitute.dsde.workbench.leonardo.http.{kubernetesProxyHost, GetAppResult}
import org.broadinstitute.dsde.workbench.leonardo.util.CacheMetrics
import org.broadinstitute.dsde.workbench.model.IP
import org.broadinstitute.dsde.workbench.model.google.GoogleProject
import org.broadinstitute.dsde.workbench.openTelemetry.OpenTelemetryMetrics

import java.util.concurrent.TimeUnit
import scala.concurrent.ExecutionContext

final case class KubernetesDnsCacheKey(googleProject: GoogleProject, appName: AppName)

/**
 * This class provides an in-memory cache of (GoogleProject, AppName) -> HostStatus.
 * This is used by ProxyService to look up the hostname to connect to for a given app proxy request.
 * It also populates HostToIpMapping reference used by JupyterNameService to match a "fake" hostname to a
 * real IP address.
 */
final class KubernetesDnsCache[F[_]: ContextShift: Logger: Timer: OpenTelemetryMetrics](
  proxyConfig: ProxyConfig,
  dbRef: DbReference[F],
  cacheConfig: CacheConfig,
  hostToIpMapping: Ref[F, Map[Host, IP]],
  blocker: Blocker
)(implicit F: Effect[F], ec: ExecutionContext) {

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
                case None            => F.pure[HostStatus](HostNotFound)
                case Some(appResult) => hostStatusByAppResult(appResult)
              }
            } yield hostStatus

            res.toIO.unsafeRunSync()
          }
        }
      )

  val recordCacheMetricsProcess: Stream[F, Unit] =
    CacheMetrics("kubernetesDnsCache")
      .process(() => Effect[F].delay(hostStatusCache.size), () => Effect[F].delay(hostStatusCache.stats))

  private def hostStatusByAppResult(appResult: GetAppResult): F[HostStatus] =
    appResult.cluster.asyncFields.map(_.loadBalancerIp) match {
      case None => F.pure[HostStatus](HostNotReady)
      case Some(ip) =>
        val h = kubernetesProxyHost(appResult.cluster, proxyConfig.proxyDomain)
        hostToIpMapping.getAndUpdate(_ + (h -> ip)).as[HostStatus](HostReady(h))
    }
}
