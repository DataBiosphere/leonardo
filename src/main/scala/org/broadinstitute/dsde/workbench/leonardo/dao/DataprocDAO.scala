package org.broadinstitute.dsde.workbench.leonardo.dao

import org.broadinstitute.dsde.workbench.leonardo.model.{ClusterName, ClusterRequest, ClusterResponse, GoogleProject}

import scala.concurrent.{ExecutionContext, Future}

trait DataprocDAO {
  def createCluster(googleProject: GoogleProject, clusterName: ClusterName, clusterRequest: ClusterRequest)(implicit executionContext: ExecutionContext): Future[ClusterResponse]
}
