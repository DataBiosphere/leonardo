package org.broadinstitute.dsde.workbench.leonardo.dns

import akka.http.scaladsl.model.Uri.Host
import cats.effect.implicits._
import cats.effect.{Blocker, ContextShift, Effect, Timer}
import cats.syntax.all._
import com.google.common.cache.{CacheBuilder, CacheLoader, CacheStats}
import fs2.Stream
import org.typelevel.log4cats.Logger
import org.broadinstitute.dsde.workbench.leonardo.config.{CacheConfig, ProxyConfig}
import org.broadinstitute.dsde.workbench.leonardo.dao.HostStatus
import org.broadinstitute.dsde.workbench.leonardo.dao.HostStatus._
import org.broadinstitute.dsde.workbench.leonardo.db.{clusterQuery, DbReference}
import org.broadinstitute.dsde.workbench.leonardo.util.CacheMetrics
import org.broadinstitute.dsde.workbench.leonardo.{GoogleId, Runtime, RuntimeName}
import org.broadinstitute.dsde.workbench.model.google.GoogleProject
import org.broadinstitute.dsde.workbench.openTelemetry.OpenTelemetryMetrics

import java.util.concurrent.TimeUnit
import scala.concurrent.ExecutionContext

final case class RuntimeDnsCacheKey(googleProject: GoogleProject, runtimeName: RuntimeName)

/**
 * This class provides an in-memory cache of (GoogleProject, RuntimeName) -> HostStatus.
 * This is used by ProxyService to look up the hostname to connect to for a given runtime
 * proxy request.
 * It also populates HostToIpMapping reference used by JupyterNameService to match a "fake" hostname to a
 * real IP address.
 */
class RuntimeDnsCache[F[_]: ContextShift: Logger: Timer: OpenTelemetryMetrics](
  proxyConfig: ProxyConfig,
  dbRef: DbReference[F],
  cacheConfig: CacheConfig,
  blocker: Blocker
)(implicit F: Effect[F], ec: ExecutionContext) {

  def getHostStatus(key: RuntimeDnsCacheKey): F[HostStatus] =
    blocker.blockOn(Effect[F].delay(projectClusterToHostStatus.get(key)))
  def size: Long = projectClusterToHostStatus.size
  def stats: CacheStats = projectClusterToHostStatus.stats

  private val projectClusterToHostStatus = CacheBuilder
    .newBuilder()
    .expireAfterWrite(cacheConfig.cacheExpiryTime.toSeconds, TimeUnit.SECONDS)
    .maximumSize(cacheConfig.cacheMaxSize)
    .recordStats
    .build(
      new CacheLoader[RuntimeDnsCacheKey, HostStatus] {
        def load(key: RuntimeDnsCacheKey): HostStatus = {
          val res = for {
            _ <- Logger[F].debug(s"DNS Cache miss for ${key.googleProject} / ${key.runtimeName}...loading from DB...")
            runtimeOpt <- dbRef.inTransaction {
              clusterQuery.getActiveClusterByNameMinimal(key.googleProject, key.runtimeName)
            }
            hostStatus = runtimeOpt match {
              case Some(runtime) =>
                hostStatusByProjectAndCluster(runtime)
              case None =>
                HostNotFound
            }
          } yield hostStatus

          res.toIO.unsafeRunSync()
        }
      }
    )

  val recordCacheMetricsProcess: Stream[F, Unit] =
    CacheMetrics("runtimeDnsCache")
      .process(() => Effect[F].delay(projectClusterToHostStatus.size),
               () => Effect[F].delay(projectClusterToHostStatus.stats))

  private def host(googleId: GoogleId): Host =
    Host(googleId.value + proxyConfig.proxyDomain)

  private def hostStatusByProjectAndCluster(r: Runtime): HostStatus = {
    val hostAndIpOpt = for {
      a <- r.asyncRuntimeFields
      h = host(a.googleId)
      ip <- a.hostIp
    } yield (h, ip)

    hostAndIpOpt match {
      case Some((h, ip)) =>
        HostReady(h, ip)
      case None =>
        if (r.status.isStartable)
          HostPaused
        else
          HostNotReady
    }
  }
}
