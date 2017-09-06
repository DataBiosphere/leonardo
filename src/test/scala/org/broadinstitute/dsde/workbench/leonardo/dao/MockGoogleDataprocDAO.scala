package org.broadinstitute.dsde.workbench.leonardo.dao

import java.util.UUID

import com.google.api.services.compute.model.Instance
import com.google.api.services.dataproc.model.{Cluster => GoogleCluster, _}
import org.broadinstitute.dsde.workbench.leonardo.model.ModelTypes.GoogleProject
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

  override def deleteCluster(googleProject: String, clusterName: String)(implicit executionContext: ExecutionContext): Future[Option[Operation]] = {
    if(clusters.contains(clusterName))
      clusters.remove(clusterName)
    Future(None)
  }

  override def getCluster(googleProject: GoogleProject, clusterName: String)(implicit executionContext: ExecutionContext): Future[GoogleCluster] = ???

  override def getOperation(operationName: String)(implicit executionContext: ExecutionContext): Future[Operation] = ???

  override def getInstance(googleProject: GoogleProject, instanceName: String)(implicit executionContext: ExecutionContext): Future[Instance] = ???
}
