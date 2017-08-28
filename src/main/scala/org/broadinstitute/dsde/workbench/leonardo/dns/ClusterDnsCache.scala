package org.broadinstitute.dsde.workbench.leonardo.dns

import akka.actor.{Actor, Props}
import akka.pattern.pipe
import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.dsde.workbench.leonardo.config.ProxyConfig
import org.broadinstitute.dsde.workbench.leonardo.db.DbReference
import org.broadinstitute.dsde.workbench.leonardo.dns.ClusterDnsCache.{GetByProjectAndName, ProcessClusters, RefreshFromDatabase}
import org.broadinstitute.dsde.workbench.leonardo.model.Cluster
import org.broadinstitute.dsde.workbench.leonardo.model.ModelTypes.GoogleProject

import scala.concurrent.Future
import scala.concurrent.duration._

/**
  * Created by rtitle on 8/25/17.
  */
object ClusterDnsCache {

  // This is stored as volatile in the object instead of inside the actor because it needs to be
  // accessed by JupyterNameService. JupyterNameService is instantiated by the container and is
  // stateless, so it doesn't have an ExecutionContext, etc needed to interact with an Actor.
  @volatile var HostToIp: Map[String, String] = Map.empty

  def props(proxyConfig: ProxyConfig, dbRef: DbReference): Props =
    Props(new ClusterDnsCache(proxyConfig, dbRef))

  sealed trait ClusterDnsCacheMessage
  case object RefreshFromDatabase extends ClusterDnsCacheMessage
  case class ProcessClusters(clusters: Seq[Cluster]) extends ClusterDnsCacheMessage
  case class GetByProjectAndName(googleProject: GoogleProject, clusterName: String) extends ClusterDnsCacheMessage
}

class ClusterDnsCache(proxyConfig: ProxyConfig, dbRef: DbReference) extends Actor with LazyLogging {
  var ProjectNameToHost: Map[(GoogleProject, String), String] = Map.empty

  import context.dispatcher

  override def preStart(): Unit = {
    super.preStart()
    scheduleRefresh
  }

  override def receive: Receive = {
    case RefreshFromDatabase =>
      queryForClusters pipeTo self
    case ProcessClusters(clusters) =>
      storeClusters(clusters)
      scheduleRefresh
    case GetByProjectAndName(googleProject, clusterName) =>
      sender ! ProjectNameToHost.get(googleProject, clusterName)
  }

  def scheduleRefresh = {
    context.system.scheduler.scheduleOnce(1 minute)(RefreshFromDatabase)
  }

  def queryForClusters: Future[ProcessClusters] ={
    dbRef.inTransaction(_.clusterQuery.list()).map(ProcessClusters)
  }

  def processClusters(clusters: Seq[Cluster]): Unit = {
    clusters.partition(_.hostIp.isDefined) match {
      case (hasIp, noIp /* TODO GAWB-2498 */) =>
        storeClusters(hasIp)
    }
  }

  def storeClusters(clusters: Seq[Cluster]): Unit = {
    ClusterDnsCache.HostToIp = clusters.map(c => c.googleId.toString + proxyConfig.jupyterDomain -> c.hostIp.get).toMap
    ProjectNameToHost = clusters.map(c => (c.googleProject, c.clusterName) -> (c.googleId.toString + proxyConfig.jupyterDomain)).toMap
    logger.info("Saved {} clusters to DNS cache", clusters.size)
  }

}