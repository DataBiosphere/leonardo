package org.broadinstitute.dsde.workbench.leonardo.dns

import akka.actor.{Actor, Props}
import akka.http.scaladsl.model.Uri.Host
import akka.pattern.pipe
import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.dsde.workbench.leonardo.config.ProxyConfig
import org.broadinstitute.dsde.workbench.leonardo.db.DbReference
import org.broadinstitute.dsde.workbench.leonardo.dns.ClusterDnsCache._
import org.broadinstitute.dsde.workbench.leonardo.model._
import org.broadinstitute.dsde.workbench.leonardo.model.google.{ClusterName, IP}
import org.broadinstitute.dsde.workbench.leonardo.service.ClusterNotReadyException
import org.broadinstitute.dsde.workbench.model.google.GoogleProject

import scala.concurrent.Future

object ClusterDnsCache {
  // This is stored as volatile in the object instead of inside the actor because it needs to be
  // accessed by JupyterNameService. JupyterNameService is instantiated by the container and is
  // stateless, so it doesn't have an ExecutionContext, etc needed to interact with an Actor.

  @volatile var HostToIp: Map[Host, IP] = Map.empty

  def props(proxyConfig: ProxyConfig, dbRef: DbReference): Props =
    Props(new ClusterDnsCache(proxyConfig, dbRef))

  // Actor messages:
  sealed trait ClusterDnsCacheMessage
  case object RefreshFromDatabase extends ClusterDnsCacheMessage
  case class ProcessClusters(clusters: Seq[Cluster]) extends ClusterDnsCacheMessage
  case class ProcessReadyCluster(cluster: Cluster) extends ClusterDnsCacheMessage
  case class GetByProjectAndName(googleProject: GoogleProject, clusterName: ClusterName) extends ClusterDnsCacheMessage

  // Responses to GetByProjectAndName message
  sealed trait GetClusterResponse
  case object ClusterNotFound extends GetClusterResponse
  case object ClusterNotReady extends GetClusterResponse
  case class ClusterReady(hostname: Host) extends GetClusterResponse
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
  var ProjectNameToHost: Map[(GoogleProject, ClusterName), GetClusterResponse] = Map.empty

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

    // add one ready cluster to the caches now instead of waiting for the refresh cycle
    case ProcessReadyCluster(cluster) =>
      sender ! processReadyCluster(cluster)

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

  private def host(c: Cluster): Host = Host(c.googleId.toString + proxyConfig.jupyterDomain)

  private def hostToIpEntry(c: Cluster): (Host, IP) = host(c) -> c.hostIp.get

  private def projectNameToHostEntry(c: Cluster): ((GoogleProject, ClusterName), GetClusterResponse) = {
    if (c.hostIp.isDefined)
      (c.googleProject, c.clusterName) -> ClusterReady(host(c))
    else
      (c.googleProject, c.clusterName) -> ClusterNotReady
  }

  def processClusters(clusters: Seq[Cluster]): Unit = {
    // Only populate the HostToIp map for clusters with an IP address
    val clustersWithIp = clusters.filter(_.hostIp.isDefined)
    ClusterDnsCache.HostToIp = clustersWithIp.map(hostToIpEntry).toMap

    // Populate the ProjectNameToHost map with all clusters
    ProjectNameToHost = clusters.map(projectNameToHostEntry).toMap

    logger.info(s"Saved ${clusters.size} clusters to DNS cache, ${clustersWithIp.size} with IPs")
    logger.info("ProjectNameToHost map = " + ProjectNameToHost)
    logger.info("HostToIP map = " + HostToIp)
  }

  def processReadyCluster(cluster: Cluster): Either[Throwable, GetClusterResponse] = {
    if (cluster.hostIp.isEmpty) {
      Left(ClusterNotReadyException(cluster.googleProject, cluster.clusterName))
    } else {
      ClusterDnsCache.HostToIp += hostToIpEntry(cluster)
      ProjectNameToHost += projectNameToHostEntry(cluster)

      logger.info(s"Saved new cluster ${cluster.projectNameString} to DNS cache with IP ${cluster.hostIp.get.value}")
      logger.info("ProjectNameToHost map = " + ProjectNameToHost)
      logger.info("HostToIP map = " + HostToIp)
      Right(ProjectNameToHost(cluster.googleProject, cluster.clusterName))
    }
  }
}