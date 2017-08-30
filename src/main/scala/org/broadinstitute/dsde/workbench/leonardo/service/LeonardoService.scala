package org.broadinstitute.dsde.workbench.leonardo.service

import akka.http.scaladsl.model.StatusCodes
import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.dsde.workbench.leonardo.dao.DataprocDAO
import org.broadinstitute.dsde.workbench.leonardo.model.{Cluster, ClusterRequest, ClusterResponse, ClusterStatus, LeoException}
import java.util.UUID
import org.broadinstitute.dsde.workbench.leonardo.db.{DataAccess, DbReference}
import org.broadinstitute.dsde.workbench.leonardo.model.ModelTypes.GoogleProject
import slick.dbio.DBIO

import scala.concurrent.{ExecutionContext, Future}

case class ClusterNotFoundException(googleProject: GoogleProject, clusterName: String) extends LeoException(s"Cluster $googleProject/$clusterName not found", StatusCodes.NotFound)

class LeonardoService(gdDAO: DataprocDAO, dbRef: DbReference)(implicit val executionContext: ExecutionContext) extends LazyLogging {

  protected def getCluster(googleProject: GoogleProject, clusterName: String, dataAccess: DataAccess): DBIO[Cluster] = {
    dataAccess.clusterQuery.getByName(googleProject, clusterName) flatMap {
      case None => throw ClusterNotFoundException(googleProject, clusterName)
      case Some(cluster) => DBIO.successful(cluster)
    }
  }

  def createCluster(googleProject: String, clusterName: String, clusterRequest: ClusterRequest): Future[Cluster] = {
    gdDAO.createCluster(googleProject, clusterName, clusterRequest) flatMap { clusterResponse: ClusterResponse =>
      dbRef.inTransaction { dataAccess =>
        dataAccess.clusterQuery.save(Cluster(clusterRequest, clusterResponse))
      }
    }
  }

  def getClusterDetails(googleProject: GoogleProject, clusterName: String): Future[Cluster] = {
    dbRef.inTransaction { dataAccess =>
      getCluster(googleProject, clusterName, dataAccess)
    }
  }
  def deleteCluster(googleProject: GoogleProject, clusterName: String): Future[Int] = {
    getClusterDetails(googleProject, clusterName) flatMap  { cluster:Cluster =>
        if(cluster.status != ClusterStatus.Deleting && cluster.status != ClusterStatus.Deleted) {
          for {
            c <- gdDAO.deleteCluster(googleProject, clusterName)
            recordCount <- dbRef.inTransaction(component => component.clusterQuery.deleteCluster(UUID.fromString(c.googleId)))
          } yield recordCount
        } else Future.successful(0)
    }
  }
}
