package org.broadinstitute.dsde.workbench.leonardo.service

import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.dsde.workbench.leonardo.dao.DataprocDAO
import org.broadinstitute.dsde.workbench.leonardo.db.DbReference
import org.broadinstitute.dsde.workbench.leonardo.model.{Cluster, ClusterRequest, ClusterResponse}

import scala.concurrent.{ExecutionContext, Future}

class LeonardoService(gdDAO: DataprocDAO, dbRef: DbReference)(implicit val executionContext: ExecutionContext) extends LazyLogging {

  def createCluster(googleProject: String, clusterName: String, clusterRequest: ClusterRequest): Future[Cluster] = {
    gdDAO.createCluster(googleProject, clusterName, clusterRequest) flatMap { clusterResponse: ClusterResponse =>
      dbRef.inTransaction { dataAccess =>
        dataAccess.clusterQuery.save(Cluster(clusterRequest, clusterResponse))
      }
    }
  }

  def getClusterDetails(googleProject: String, clusterName: String): Future[Cluster] = {
    dbRef.inTransaction { components =>
      components.clusterQuery.getByName(googleProject, clusterName) map( _.get )
    }
  }

}
