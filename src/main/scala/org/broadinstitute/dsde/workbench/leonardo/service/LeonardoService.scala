package org.broadinstitute.dsde.workbench.leonardo.service

import akka.http.scaladsl.model.StatusCodes
import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.dsde.workbench.leonardo.dao.DataprocDAO
import org.broadinstitute.dsde.workbench.leonardo.db.DataAccess
import org.broadinstitute.dsde.workbench.leonardo.model.ModelTypes.GoogleProject
import org.broadinstitute.dsde.workbench.leonardo.model.{LeoException}
import slick.dbio.DBIO
import org.broadinstitute.dsde.workbench.leonardo.db.DbReference
import org.broadinstitute.dsde.workbench.leonardo.model.{Cluster, ClusterRequest, ClusterResponse}

import scala.concurrent.{ExecutionContext, Future}

case class ClusterNotFoundException(googleProject: GoogleProject, clusterName: String) extends LeoException(s"Cluster $googleProject/$clusterName not found", StatusCodes.NotFound)
case class ClusterAlreadyExistsException(googleProject: GoogleProject, clusterName: String) extends LeoException(s"Cluster$googleProject/$clusterName already exists", StatusCodes.Found)

class LeonardoService(gdDAO: DataprocDAO, dbRef: DbReference)(implicit val executionContext: ExecutionContext) extends LazyLogging {

  protected def getCluster(googleProject: GoogleProject, clusterName: String, dataAccess: DataAccess): DBIO[Cluster] = {
    dataAccess.clusterQuery.getByName(googleProject, clusterName) flatMap {
      case None => throw ClusterNotFoundException(googleProject, clusterName)
      case Some(cluster) => DBIO.successful(cluster)
    }
  }

  def createCluster(googleProject: String, clusterName: String, clusterRequest: ClusterRequest): Future[Cluster] = {
    def create() =
      gdDAO.createCluster(googleProject, clusterName, clusterRequest) flatMap { clusterResponse: ClusterResponse =>
        dbRef.inTransaction { dataAccess =>
          dataAccess.clusterQuery.save(Cluster(clusterRequest, clusterResponse))
        }
      }

    dbRef.inTransaction { dataAccess => dataAccess.clusterQuery.getByName(googleProject, clusterName) } flatMap {
      case Some(c: Cluster) => throw ClusterAlreadyExistsException(googleProject, clusterName)
      case None => create
    }

  }

  def getClusterDetails(googleProject: GoogleProject, clusterName: String): Future[Cluster] = {
    dbRef.inTransaction { dataAccess =>
      getCluster(googleProject, clusterName, dataAccess)
    }
  }

  def deleteCluster(googleProject: GoogleProject, clusterName: String): Future[Int] = {
    getClusterDetails(googleProject, clusterName) flatMap  { cluster =>
      if(cluster.status.isActive) {
        for {
          _ <- gdDAO.deleteCluster(googleProject, clusterName)
          recordCount <- dbRef.inTransaction(dataAccess => dataAccess.clusterQuery.deleteCluster(cluster.googleId))
        } yield recordCount
      } else Future.successful(0)
    }
  }
}
