package org.broadinstitute.dsde.workbench.leonardo.service

import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.dsde.workbench.leonardo.dao.DataprocDAO
import org.broadinstitute.dsde.workbench.leonardo.db.DbReference
import org.broadinstitute.dsde.workbench.leonardo.model._

import scala.concurrent.{ExecutionContext, Future}

class LeonardoService(gdDAO: DataprocDAO, dbRef: DbReference)(implicit val executionContext: ExecutionContext) extends LazyLogging {

  def createCluster(googleProject: GoogleProject, clusterName: ClusterName, clusterRequest: ClusterRequest): Future[Cluster] = {
    gdDAO.createCluster(googleProject, clusterName, clusterRequest) flatMap { clusterResponse: ClusterResponse =>
      dbRef.inTransaction { components =>
        components.clusterQuery.save(Cluster(clusterRequest, clusterResponse))
      }
    }
  }

}
