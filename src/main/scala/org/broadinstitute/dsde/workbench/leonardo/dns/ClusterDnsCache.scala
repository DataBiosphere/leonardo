package org.broadinstitute.dsde.workbench.leonardo.dns

import akka.actor.{Actor, Props}
import akka.pattern.pipe
import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.dsde.workbench.leonardo.config.ProxyConfig
import org.broadinstitute.dsde.workbench.leonardo.db.DbReference
import org.broadinstitute.dsde.workbench.leonardo.dns.ClusterDnsCache._
import org.broadinstitute.dsde.workbench.leonardo.model.Cluster
import org.broadinstitute.dsde.workbench.leonardo.model.ModelTypes.GoogleProject
import scala.concurrent.Future

object ClusterDnsCache {
  // This is stored as volatile in the object instead of inside the actor because it needs to be
  // accessed by JupyterNameService. JupyterNameService is instantiated by the container and is
  // stateless, so it doesn't have an ExecutionContext, etc needed to interact with an Actor.
  @volatile var HostToIp: Map[String, String] = Map.empty

  def props(proxyConfig: ProxyConfig, dbRef: DbReference): Props =
    Props(new ClusterDnsCache(proxyConfig, dbRef))

  // Actor messages:
  sealed trait ClusterDnsCacheMessage
  case object RefreshFromDatabase extends ClusterDnsCacheMessage
  case class ProcessClusters(clusters: Seq[Cluster]) extends ClusterDnsCacheMessage
  case class GetByProjectAndName(googleProject: GoogleProject, clusterName: String) extends ClusterDnsCacheMessage

  // Responses to GetByProjectAndName message
  sealed trait GetClusterResponse
  case object ClusterNotFound extends GetClusterResponse
  case object ClusterNotReady extends GetClusterResponse
  case class ClusterReady(hostname: String) extends GetClusterResponse
}

/**
  * This actor periodically queries the DB for all clusters, and updates in-memory caches of:
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
class ClusterDnsCache(proxyConfig: ProxyConfig, dbRef: DbReference) extends Actor with LazyLogging {
  var ProjectNameToHost: Map[(GoogleProject, String), GetClusterResponse] = Map.empty

  import context.dispatcher

  override def preStart(): Unit = {
    super.preStart()
    self ! RefreshFromDatabase
  }

  override def receive: Receive = {
    case RefreshFromDatabase =>
      queryForClusters pipeTo self

    case ProcessClusters(clusters) =>
      processClusters(clusters)
      scheduleRefresh

    case GetByProjectAndName(googleProject, clusterName) =>
      sender ! ProjectNameToHost.get((googleProject, clusterName)).getOrElse(ClusterNotFound)
  }

  def scheduleRefresh = {
    context.system.scheduler.scheduleOnce(proxyConfig.dnsPollPeriod, self, RefreshFromDatabase)
  }

  def queryForClusters: Future[ProcessClusters] ={
    dbRef.inTransaction { dataAccess =>
      dataAccess.clusterQuery.listActive()
    }.map(ProcessClusters.apply)
  }

  def processClusters(clusters: Seq[Cluster]): Unit = {
    // Only populate the HostToIp map for clusters with an IP address
    val clustersWithIp = clusters.filter(_.hostIp.isDefined)
    ClusterDnsCache.HostToIp = clustersWithIp.map(c => c.googleId.toString + proxyConfig.jupyterDomain -> c.hostIp.get).toMap

    // Populate the ProjectNameToHost map with all clusters
    ProjectNameToHost = clusters.map {
      case c if c.hostIp.isDefined =>
        (c.googleProject, c.clusterName) -> ClusterReady(c.googleId.toString + proxyConfig.jupyterDomain)
      case c =>
        (c.googleProject, c.clusterName) -> ClusterNotReady
    }.toMap

    logger.debug(s"Saved ${clusters.size} clusters to DNS cache, ${clustersWithIp.size} with IPs")
  }

}