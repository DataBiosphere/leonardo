package org.broadinstitute.dsde.workbench.leonardo.dao

import com.google.api.services.dataproc.model.Operation
import org.broadinstitute.dsde.workbench.leonardo.model.{ClusterRequest, ClusterResponse}

import scala.concurrent.{ExecutionContext, Future}

trait DataprocDAO {
  def createCluster(googleProject: String, clusterName: String, clusterRequest: ClusterRequest)(implicit executionContext: ExecutionContext): Future[ClusterResponse]
}
