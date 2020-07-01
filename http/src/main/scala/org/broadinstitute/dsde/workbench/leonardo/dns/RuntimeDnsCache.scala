package org.broadinstitute.dsde.workbench.leonardo.dns

import java.util.concurrent.TimeUnit

import akka.http.scaladsl.model.Uri.Host
import cats.effect.implicits._
import cats.effect.{Blocker, ContextShift, Effect}
import com.google.common.cache.{CacheBuilder, CacheLoader, CacheStats}
import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.dsde.workbench.leonardo.config.{CacheConfig, ProxyConfig}
import org.broadinstitute.dsde.workbench.leonardo.dao.HostStatus
import org.broadinstitute.dsde.workbench.leonardo.dao.HostStatus._
import org.broadinstitute.dsde.workbench.leonardo.db.{clusterQuery, DbReference}
import org.broadinstitute.dsde.workbench.leonardo.{IP, Runtime, RuntimeName}
import org.broadinstitute.dsde.workbench.model.google.GoogleProject

import scala.concurrent.ExecutionContext

final case class RuntimeDnsCacheKey(googleProject: GoogleProject, runtimeName: RuntimeName)

/**
 * This class provides in-memory caches of:
 * 1. (GoogleProject, RuntimeName) -> HostStatus
 *    This is used by ProxyService to look up the hostname to connect to given the GoogleProject/RuntimeName
 *    in the Leo request URI.
 * 2. Hostname -> IP
 *    This is used by JupyterNameService to match a "fake" hostname to a real IP address. Note
 *    this cache is in the object (instead of the class) because of the way
 *    JupyterNameService needs to access the singleton.
 * @param proxyConfig the proxy configuration
 * @param dbRef provides access to the database
 */
class RuntimeDnsCache[F[_]: Effect: ContextShift](proxyConfig: ProxyConfig,
                                                  dbRef: DbReference[F],
                                                  cacheConfig: CacheConfig,
                                                  blocker: Blocker)(implicit ec: ExecutionContext)
    extends LazyLogging {

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
          logger.debug(s"DNS Cache miss for ${key.googleProject} / ${key.runtimeName}...loading from DB...")
          val res = dbRef
            .inTransaction {
              clusterQuery.getActiveClusterByNameMinimal(key.googleProject, key.runtimeName)
            }
            .toIO
            .unsafeRunSync()

          res match {
            case Some(runtime) =>
              getHostStatusAndUpdateHostToIpIfHostReady(runtime)
            case None =>
              HostNotFound
          }
        }
      }
    )

  private def getHostStatusAndUpdateHostToIpIfHostReady(runtime: Runtime): HostStatus = {
    val hostStatus = hostStatusByProjectAndCluster(runtime)

    PartialFunction.condOpt(hostStatus) {
      case HostReady(_) => HostToIpMapping.hostToIpMapping.mutate(_ + hostToIpEntry(runtime))
    }

    hostStatus
  }

  private def host(r: Runtime): Host = {
    val googleId = r.asyncRuntimeFields.map(_.googleId)
    val assumption = s"Google ID for Google project/cluster ${r.googleProject}/${r.runtimeName} must not be undefined."
    assert(googleId.isDefined, assumption)

    Host(googleId.get.value + proxyConfig.proxyDomain)
  }

  private def hostToIpEntry(r: Runtime): (Host, IP) = host(r) -> r.asyncRuntimeFields.flatMap(_.hostIp).get

  private def hostStatusByProjectAndCluster(r: Runtime): HostStatus =
    if (r.asyncRuntimeFields.flatMap(_.hostIp).isDefined)
      HostReady(host(r))
    else if (r.status.isStartable)
      HostPaused
    else
      HostNotReady

}
