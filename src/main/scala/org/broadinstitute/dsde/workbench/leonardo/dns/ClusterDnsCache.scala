package org.broadinstitute.dsde.workbench.leonardo.dns

import akka.actor.ActorSystem
import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.dsde.workbench.leonardo.config.ProxyConfig
import org.broadinstitute.dsde.workbench.leonardo.db.DbReference
import org.broadinstitute.dsde.workbench.leonardo.model.Cluster
import org.broadinstitute.dsde.workbench.leonardo.model.ModelTypes.GoogleProject

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._
import scala.util.control.NonFatal
import scala.util.{Failure, Success, Try}

/**
  * Created by rtitle on 8/25/17.
  */
object ClusterDnsCache extends LazyLogging {

  @volatile var HostToIp: Map[String, String] = Map.empty
  @volatile var ProjectNameToHost: Map[(GoogleProject, String), String] = Map.empty

  // Needs to match the wildcard cert
  // TODO configify
  private val HostSuffix = ".jupyter.firecloud.org"

  def init(proxyConfig: ProxyConfig, dbRef: DbReference)(implicit system: ActorSystem, executionContext: ExecutionContext) = {
    system.scheduler.schedule(1 seconds, 1 seconds) {
      val allClusters = dbRef.inTransaction(_.clusterQuery.list())
      allClusters onComplete save
    }
  }

  def save(allClusters: Try[Seq[Cluster]]): Unit = {
    allClusters match {
      case Success(clusters) =>
        val filteredClusters = clusters.filter(_.hostIp.isDefined)

        HostToIp = filteredClusters.map(c => c.googleId.toString + HostSuffix -> c.hostIp.get).toMap
        ProjectNameToHost = filteredClusters.map(c => (c.googleProject, c.clusterName) -> (c.googleId.toString + HostSuffix)).toMap

        logger.info("Saved {} clusters to DNS cache", filteredClusters.size)
      case Failure(NonFatal(t)) =>
        logger.error("Error populating DNS cache", t)
    }
  }

}
