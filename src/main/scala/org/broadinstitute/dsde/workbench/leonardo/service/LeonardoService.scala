package org.broadinstitute.dsde.workbench.leonardo.service

import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.dsde.workbench.leonardo.dao.DataprocDAO
import org.broadinstitute.dsde.workbench.leonardo.model.{ClusterRequest, ClusterResponse}

import scala.concurrent.{ExecutionContext, Future}

class LeonardoService(gdDAO: DataprocDAO)(implicit val executionContext: ExecutionContext) extends LazyLogging {

  def createCluster(googleProject: String, clusterName: String, clusterRequest: ClusterRequest): Future[ClusterResponse] = {
    gdDAO.createCluster(googleProject, clusterName, clusterRequest)
  }

}
