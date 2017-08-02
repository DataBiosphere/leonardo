package org.broadinstitute.dsde.workbench.leonardo.dao

import com.google.api.services.dataproc.model.Operation
import org.broadinstitute.dsde.workbench.leonardo.model.ClusterRequest
import scala.concurrent.Future

trait DataprocDAO {
  def createCluster(googleProject: String, clusterName: String, clusterRequest: ClusterRequest): Future[Operation]
}
