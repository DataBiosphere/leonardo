package org.broadinstitute.dsde.workbench.leonardo.dns

import java.util.concurrent.TimeUnit

import akka.http.scaladsl.model.Uri.Host
import com.google.common.cache.{CacheBuilder, CacheLoader}
import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.dsde.workbench.leonardo.config.{ClusterDnsCacheConfig, ProxyConfig}
import org.broadinstitute.dsde.workbench.leonardo.db.DbReference
import org.broadinstitute.dsde.workbench.leonardo.dns.ClusterDnsCache._
import org.broadinstitute.dsde.workbench.leonardo.model._
import org.broadinstitute.dsde.workbench.leonardo.model.google.{ClusterName, IP}
import org.broadinstitute.dsde.workbench.leonardo.util.ValueBox
import org.broadinstitute.dsde.workbench.model.google.GoogleProject

import scala.concurrent.{ExecutionContext, Future}

object ClusterDnsCache {
  // This is stored as volatile in the object instead of inside the actor because it needs to be
  // accessed by JupyterNameService. JupyterNameService is instantiated by the container and is
  // stateless, so it doesn't have an ExecutionContext, etc needed to interact with an Actor.

  val HostToIp: ValueBox[Map[Host, IP]] = ValueBox(Map.empty)

  sealed trait HostStatus
  case object HostNotFound extends HostStatus
  case object HostNotReady extends HostStatus
  case object HostPaused extends HostStatus
  case class HostReady(hostname: Host) extends HostStatus
}

case class DnsCacheKey(googleProject: GoogleProject, clusterName: ClusterName)

/**
  * This class provides in-memory caches of:
  * 1. (GoogleProject, ClusterName) -> Hostname
  *    This is used by ProxyService to look up the hostname to connect to given the GoogleProject/ClusterName
  *    in the Leo request URI.
  * 2. Hostname -> IP
  *    This is used by JupyterNameService to match a "fake" hostname to a real IP address. Note
  *    this cache is in the object not the Actor because JupyterNameService doesn't have an ExecutionContext
  *    to query the Actor.
  * @param proxyConfig the proxy configuration
  * @param dbRef provides access to the database
  */
class ClusterDnsCache(proxyConfig: ProxyConfig, dbRef: DbReference, dnsCacheConfig: ClusterDnsCacheConfig)(implicit executionContext: ExecutionContext) extends LazyLogging {

  val projectNameToHost = CacheBuilder.newBuilder()
    .expireAfterWrite(dnsCacheConfig.cacheExpiryTime.toSeconds, TimeUnit.SECONDS)
    .maximumSize(dnsCacheConfig.cacheMaxSize)
    .build(
      new CacheLoader[DnsCacheKey, Future[HostStatus]] {
        def load(key: DnsCacheKey) = {
          dbRef.inTransaction {dataAccess => dataAccess.clusterQuery.getActiveClusterByName(key.googleProject, key.clusterName)}.map {
              case  Some(cluster) => {
                val res = projectNameToHostEntry(cluster)._2
                if(res.isInstanceOf[HostReady]) {
                  ClusterDnsCache.HostToIp.value = Map(hostToIpEntry(cluster))
                }
                res
              }
              case None => HostNotFound
          }
        }
      }
    )

  private def host(c: Cluster): Host = {
    val googleId = c.dataprocInfo.googleId
    val assumption = s"Google ID for Google project/cluster ${c.googleProject}/${c.clusterName} must not be undefined."
    assert(googleId.isDefined, assumption)

    Host(googleId.get.toString + proxyConfig.jupyterDomain)
  }

  private def hostToIpEntry(c: Cluster): (Host, IP) = host(c) -> c.dataprocInfo.hostIp.get

  private def projectNameToHostEntry(c: Cluster): ((GoogleProject, ClusterName), HostStatus) = {
    if (c.status.isStartable)
      (c.googleProject, c.clusterName) -> HostPaused
    else if (c.dataprocInfo.hostIp.isDefined)
      (c.googleProject, c.clusterName) -> HostReady(host(c))
    else
      (c.googleProject, c.clusterName) -> HostNotReady
  }

}
