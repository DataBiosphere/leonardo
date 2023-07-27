package org.broadinstitute.dsde.workbench.leonardo
package dns

import akka.http.scaladsl.model.Uri.Host
import cats.effect.{Async, Ref}
import cats.syntax.all._
import org.broadinstitute.dsde.workbench.leonardo.config.ProxyConfig
import org.broadinstitute.dsde.workbench.leonardo.dao.HostStatus
import org.broadinstitute.dsde.workbench.leonardo.dao.HostStatus._
import org.broadinstitute.dsde.workbench.leonardo.db.{clusterQuery, ClusterRecord, DbReference}
import org.broadinstitute.dsde.workbench.model.IP
import org.broadinstitute.dsde.workbench.openTelemetry.OpenTelemetryMetrics
import org.typelevel.log4cats.Logger
import scalacache.Cache

import scala.concurrent.ExecutionContext

final case class RuntimeDnsCacheKey(cloudContext: CloudContext, runtimeName: RuntimeName)

/**
 * This class provides an in-memory cache of (GoogleProject, RuntimeName) -> HostStatus.
 * This is used by ProxyService to look up the hostname to connect to for a given runtime
 * proxy request.
 * It also populates HostToIpMapping reference used by JupyterNameService to match a "fake" hostname to a
 * real IP address.
 * TODO [IA-4460] watch this cache for "miss storms" where multiple threads miss the cache, starting a positive
 * feedback loop where queries run slower, take longer, and extend the time the cache is cold, accruing more
 * misses. Consider keyed blocking via a mutex, or increasing time-to-live ({@code CacheConfig::cacheExpiryTime}).
 */
class RuntimeDnsCache[F[_]: Logger: OpenTelemetryMetrics](
  proxyConfig: ProxyConfig,
  dbRef: DbReference[F],
  hostToIpMapping: Ref[F, Map[Host, IP]],
  runtimeDnsCache: Cache[F, RuntimeDnsCacheKey, HostStatus]
)(implicit F: Async[F], ec: ExecutionContext) {
  def getHostStatus(key: RuntimeDnsCacheKey): F[HostStatus] =
    runtimeDnsCache.cachingF(key)(None)(getHostStatusHelper(key))

  private def getHostStatusHelper(key: RuntimeDnsCacheKey): F[HostStatus] =
    for {
      _ <- Logger[F]
        .debug(s"DNS Cache miss for ${key.cloudContext} / ${key.runtimeName}...loading from DB...")
      runtimeOpt <- dbRef.inTransaction {
        clusterQuery.getActiveClusterRecordByName(key.cloudContext, key.runtimeName)
      }
      hostStatus <- runtimeOpt match {
        case Some(runtime) =>
          key.cloudContext match {
            case x: CloudContext.Gcp =>
              hostStatusByProjectAndCluster(runtime, x, key.runtimeName)
            case _: CloudContext.Azure =>
              runtime.hostIp match {
                case Some(ip) =>
                  F.pure(HostReady(Host(s"${ip.asString}"), runtime.runtimeName.asString, CloudProvider.Azure))
                case None =>
                  if (runtime.status.isStartable)
                    F.pure[HostStatus](HostPaused)
                  else
                    F.pure[HostStatus](HostNotReady)
              }
          }

        case None =>
          F.pure[HostStatus](HostNotFound)
      }
    } yield hostStatus

  private def host(googleId: ProxyHostName): Host =
    Host(googleId.value + proxyConfig.proxyDomain)

  private def hostStatusByProjectAndCluster(r: ClusterRecord,
                                            cloudContext: CloudContext.Gcp,
                                            runtimeName: RuntimeName
  ): F[HostStatus] = {
    val hostAndIpOpt = for {
      a <- r.googleId
      h = host(a)
      ip <- r.hostIp
    } yield (h, ip)

    hostAndIpOpt match {
      case Some((h, ip)) =>
        hostToIpMapping
          .getAndUpdate(_ + (h -> ip))
          .as[HostStatus](
            HostReady(h, s"${cloudContext.asString}/${runtimeName.asString}", CloudProvider.Gcp)
          )
      case None =>
        if (r.status.isStartable)
          F.pure[HostStatus](HostPaused)
        else
          F.pure[HostStatus](HostNotReady)
    }
  }
}
