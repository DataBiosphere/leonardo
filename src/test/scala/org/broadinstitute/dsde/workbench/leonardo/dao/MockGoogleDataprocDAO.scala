package org.broadinstitute.dsde.workbench.leonardo.dao

import org.broadinstitute.dsde.workbench.leonardo.model.{ClusterRequest, ClusterResponse}

import scala.collection.concurrent.TrieMap
import scala.collection.mutable
import scala.concurrent.{ExecutionContext, Future}

class MockGoogleDataprocDAO extends DataprocDAO {

  private val clusters: mutable.Map[String, ClusterRequest] = new TrieMap()

  override def createCluster(googleProject: String, clusterName: String, clusterRequest: ClusterRequest)(implicit executionContext: ExecutionContext): Future[ClusterResponse] = {
    clusters += clusterName -> clusterRequest
    Future {
      new ClusterResponse(clusterName, googleProject, "id", "status", "desc", "op-name")}
  }

}
