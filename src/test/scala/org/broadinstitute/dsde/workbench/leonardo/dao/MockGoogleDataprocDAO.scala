package org.broadinstitute.dsde.workbench.leonardo.dao

import java.util.UUID

import org.broadinstitute.dsde.workbench.leonardo.model.{Cluster, ClusterRequest, ClusterResponse}

import scala.collection.concurrent.TrieMap
import scala.collection.mutable
import scala.concurrent.{ExecutionContext, Future}

class MockGoogleDataprocDAO extends DataprocDAO {

  private val clusters: mutable.Map[String, Cluster] = new TrieMap()

  private def googleID = UUID.randomUUID().toString

  override def createCluster(googleProject: String, clusterName: String, clusterRequest: ClusterRequest)(implicit executionContext: ExecutionContext): Future[ClusterResponse] = {
    val clusterResponse = ClusterResponse(clusterName, googleProject, googleID, "status", "desc", "op-name")

    clusters += clusterName -> Cluster(clusterRequest, clusterResponse)

    Future.successful(clusterResponse)
  }

  override def deleteCluster(googleProject: String, clusterName: String)(implicit executionContext: ExecutionContext): Future[Unit] = {
    if(clusters.contains(clusterName))
      clusters.remove(clusterName)
    Future(())
  }
}
