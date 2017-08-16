package org.broadinstitute.dsde.workbench.leonardo.dao

import java.util.UUID

import org.broadinstitute.dsde.workbench.leonardo.model._

import scala.collection.concurrent.TrieMap
import scala.collection.mutable
import scala.concurrent.{ExecutionContext, Future}

class MockGoogleDataprocDAO extends DataprocDAO {

  private val clusters: mutable.Map[ClusterName, Cluster] = new TrieMap()

  override def createCluster(googleProject: GoogleProject, clusterName: ClusterName, clusterRequest: ClusterRequest)(implicit executionContext: ExecutionContext): Future[ClusterResponse] = {
    val clusterResponse = ClusterResponse(clusterName, googleProject, UUID.randomUUID(), "status", "desc", OperationName("op-name"))

    clusters += clusterName -> Cluster(clusterRequest, clusterResponse)

    Future.successful(clusterResponse)
  }

}
