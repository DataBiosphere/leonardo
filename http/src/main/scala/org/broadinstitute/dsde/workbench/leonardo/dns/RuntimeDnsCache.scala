package org.broadinstitute.dsde.workbench.leonardo.dns

import akka.http.scaladsl.model.Uri.Host
import cats.effect.{Async, Ref}
import cats.syntax.all._
import org.broadinstitute.dsde.workbench.leonardo.config.{CacheConfig, ProxyConfig}
import org.broadinstitute.dsde.workbench.leonardo.dao.HostStatus
import org.broadinstitute.dsde.workbench.leonardo.dao.HostStatus._
import org.broadinstitute.dsde.workbench.leonardo.db.{clusterQuery, DbReference}
import org.broadinstitute.dsde.workbench.leonardo.{GoogleId, Runtime, RuntimeName}
import org.broadinstitute.dsde.workbench.model.IP
import org.broadinstitute.dsde.workbench.model.google.GoogleProject
import org.broadinstitute.dsde.workbench.openTelemetry.OpenTelemetryMetrics
import org.typelevel.log4cats.Logger
import scalacache.Cache

import scala.concurrent.ExecutionContext

final case class RuntimeDnsCacheKey(googleProject: GoogleProject, runtimeName: RuntimeName)

/**
 * This class provides an in-memory cache of (GoogleProject, RuntimeName) -> HostStatus.
 * This is used by ProxyService to look up the hostname to connect to for a given runtime
 * proxy request.
 * It also populates HostToIpMapping reference used by JupyterNameService to match a "fake" hostname to a
 * real IP address.
 */
class RuntimeDnsCache[F[_]: Logger: OpenTelemetryMetrics](
  proxyConfig: ProxyConfig,
  dbRef: DbReference[F],
  cacheConfig: CacheConfig,
  hostToIpMapping: Ref[F, Map[Host, IP]],
  runtimeDnsCache: Cache[F, HostStatus]
)(implicit F: Async[F], ec: ExecutionContext) {
  //  TODO: Set ttl to cacheConfig.tokenCacheExpiryTime properly once https://github.com/cb372/scalacache/issues/522 in scalacache is fixed
  def getHostStatus(key: RuntimeDnsCacheKey): F[HostStatus] =
    runtimeDnsCache.cachingF(key)(None)(getHostStatusHelper(key))

  private def getHostStatusHelper(key: RuntimeDnsCacheKey): F[HostStatus] =
    for {
      _ <- Logger[F]
        .debug(s"DNS Cache miss for ${key.googleProject} / ${key.runtimeName}...loading from DB...")
      runtimeOpt <- dbRef.inTransaction {
        clusterQuery.getActiveClusterByNameMinimal(key.googleProject, key.runtimeName)
      }
      hostStatus <- runtimeOpt match {
        case Some(runtime) =>
          hostStatusByProjectAndCluster(runtime)
        case None =>
          F.pure[HostStatus](HostNotFound)
      }
    } yield hostStatus

  private def host(googleId: GoogleId): Host =
    Host(googleId.value + proxyConfig.proxyDomain)

  private def hostStatusByProjectAndCluster(r: Runtime): F[HostStatus] = {
    val hostAndIpOpt = for {
      a <- r.asyncRuntimeFields
      h = host(a.googleId)
      ip <- a.hostIp
    } yield (h, ip)

    hostAndIpOpt match {
      case Some((h, ip)) =>
        hostToIpMapping.getAndUpdate(_ + (h -> ip)).as[HostStatus](HostReady(h))
      case None =>
        if (r.status.isStartable)
          F.pure[HostStatus](HostPaused)
        else
          F.pure[HostStatus](HostNotReady)
    }
  }
}
