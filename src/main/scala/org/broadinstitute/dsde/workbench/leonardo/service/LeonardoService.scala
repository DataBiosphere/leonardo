package org.broadinstitute.dsde.workbench.leonardo.service

import akka.actor.ActorRef
import akka.http.scaladsl.model.StatusCodes
import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.dsde.workbench.leonardo.dao.DataprocDAO
import org.broadinstitute.dsde.workbench.leonardo.db.{DataAccess, DbReference}
import org.broadinstitute.dsde.workbench.leonardo.model.ModelTypes.GoogleProject
import org.broadinstitute.dsde.workbench.leonardo.model.{Cluster, ClusterRequest, ClusterResponse, LeoException}
import org.broadinstitute.dsde.workbench.leonardo.monitor.ClusterMonitorSupervisor.{ClusterCreated, ClusterDeleted}
import slick.dbio.DBIO

import scala.concurrent.{ExecutionContext, Future}
import scala.util.Success

case class ClusterNotFoundException(googleProject: GoogleProject, clusterName: String) extends LeoException(s"Cluster $googleProject/$clusterName not found", StatusCodes.NotFound)

class LeonardoService(gdDAO: DataprocDAO, dbRef: DbReference, val clusterMonitorSupervisor: ActorRef)(implicit val executionContext: ExecutionContext) extends LazyLogging {

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
    } andThen { case Success(cluster) =>
      clusterMonitorSupervisor ! ClusterCreated(cluster)
    }
  }

  def getClusterDetails(googleProject: GoogleProject, clusterName: String): Future[Cluster] = {
    dbRef.inTransaction { dataAccess =>
      getCluster(googleProject, clusterName, dataAccess)
    }
  }

  def deleteCluster(googleProject: GoogleProject, clusterName: String): Future[Unit] = {
    getClusterDetails(googleProject, clusterName) flatMap  { cluster =>
      if (cluster.status.isActive) {
        for {
          operation <- gdDAO.deleteCluster(googleProject, clusterName)
          _ <- dbRef.inTransaction { _.clusterQuery.deleteCluster(cluster.googleId, operation.map(_.getName)) }
        } yield {
          clusterMonitorSupervisor ! ClusterDeleted(cluster)
        }
      } else Future.successful(())
    }
  }
}
