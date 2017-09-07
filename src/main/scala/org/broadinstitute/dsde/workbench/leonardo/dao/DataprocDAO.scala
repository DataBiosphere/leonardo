package org.broadinstitute.dsde.workbench.leonardo.dao

import com.google.api.services.compute.model.Instance
import com.google.api.services.dataproc.model.{Cluster, Operation}
import org.broadinstitute.dsde.workbench.leonardo.model.ModelTypes.GoogleProject
import org.broadinstitute.dsde.workbench.leonardo.model.{ClusterRequest, ClusterResponse}

import scala.concurrent.{ExecutionContext, Future}

trait DataprocDAO {
  def createCluster(googleProject: String, clusterName: String, clusterRequest: ClusterRequest)(implicit executionContext: ExecutionContext): Future[ClusterResponse]

  def deleteCluster(googleProject: String, clusterName: String)(implicit executionContext: ExecutionContext): Future[Option[Operation]]

  def getCluster(googleProject: GoogleProject, clusterName: String)(implicit executionContext: ExecutionContext): Future[Cluster]

  def getOperation(operationName: String)(implicit executionContext: ExecutionContext): Future[Operation]

  def getInstance(googleProject: GoogleProject, zone: String, instanceName: String)(implicit executionContext: ExecutionContext): Future[Instance]
}
