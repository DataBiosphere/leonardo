package org.broadinstitute.dsde.workbench.leonardo.service

import java.io.File
import java.util.UUID

import akka.actor.ActorRef
import akka.http.scaladsl.model.StatusCodes
import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.dsde.workbench.leonardo.config.DataprocConfig
import org.broadinstitute.dsde.workbench.leonardo.dao._
import org.broadinstitute.dsde.workbench.leonardo.db.{DataAccess, DbReference}
import org.broadinstitute.dsde.workbench.leonardo.model.LeonardoJsonSupport._
import org.broadinstitute.dsde.workbench.leonardo.model.ModelTypes.{GoogleBucketUri, GoogleProject}
import org.broadinstitute.dsde.workbench.leonardo.model._
import org.broadinstitute.dsde.workbench.leonardo.monitor.ClusterMonitorSupervisor.{ClusterCreated, ClusterDeleted, RegisterLeoService}
import slick.dbio.DBIO
import spray.json._

import scala.concurrent.{ExecutionContext, Future}
import scala.util.Success

case class ClusterNotFoundException(googleProject: GoogleProject, clusterName: String)
  extends LeoException(s"Cluster $googleProject/$clusterName not found", StatusCodes.NotFound)

case class ClusterAlreadyExistsException(googleProject: GoogleProject, clusterName: String)
  extends LeoException(s"Cluster $googleProject/$clusterName already exists", StatusCodes.Conflict)

case class InitializationFileException(googleProject: GoogleProject, clusterName: String, errorMessage: String)
  extends LeoException(s"Unable to process initialization files for $googleProject/$clusterName. Returned message: $errorMessage", StatusCodes.Conflict)

case class TemplatingException(filePath: String, errorMessage: String)
  extends LeoException(s"Unable to template file: $filePath. Returned message: $errorMessage", StatusCodes.Conflict)

class LeonardoService(protected val dataprocConfig: DataprocConfig, gdDAO: DataprocDAO, dbRef: DbReference, val clusterMonitorSupervisor: ActorRef)(implicit val executionContext: ExecutionContext) extends LazyLogging {

  // Register this instance with the cluster monitor supervisor so our cluster monitor can potentially delete and recreate clusters
  clusterMonitorSupervisor ! RegisterLeoService(this)

  private[service] def getCluster(googleProject: GoogleProject, clusterName: String, dataAccess: DataAccess): DBIO[Cluster] = {
    dataAccess.clusterQuery.getByName(googleProject, clusterName) flatMap {
      case None => throw ClusterNotFoundException(googleProject, clusterName)
      case Some(cluster) => DBIO.successful(cluster)
    }
  }

  def createCluster(googleProject: String, clusterName: String, clusterRequest: ClusterRequest): Future[Cluster] = {
    def create() = {
      createGoogleCluster(googleProject, clusterName, clusterRequest) flatMap { clusterResponse: ClusterResponse =>
        dbRef.inTransaction { dataAccess =>
          dataAccess.clusterQuery.save(Cluster(clusterRequest, clusterResponse))
        }
      }
    }

    // Check if the google project has a cluster with the same name. If not, we can create it
    dbRef.inTransaction { dataAccess =>
      dataAccess.clusterQuery.getByName(googleProject, clusterName)
    } flatMap {
      case Some(_) => throw ClusterAlreadyExistsException(googleProject, clusterName)
      case None =>
        create andThen { case Success(cluster) =>
          clusterMonitorSupervisor ! ClusterCreated(cluster)
        }
    }
  }


  /* Creates a cluster in the given google project:
     - Add a firewall rule to the user's google project if it doesn't exist, so we can access the cluster
     - Create the initialization bucket for the cluster in the leo google project
     - Upload all the necessary initialization files to the bucket
     - Create the cluster in the google project
   Currently, the bucketPath of the clusterRequest is not used - it will be used later as a place to store notebook results */
  private[service] def createGoogleCluster(googleProject: GoogleProject, clusterName: String, clusterRequest: ClusterRequest)(implicit executionContext: ExecutionContext): Future[ClusterResponse] = {
    val bucketName = s"${clusterName}-${UUID.randomUUID.toString}"
    for {
      // Create the firewall rule in the google project if it doesn't already exist, so we can access the cluster
      _ <- gdDAO.updateFirewallRule(googleProject)
      // Create the bucket in leo's google bucket and populate with initialization files
      _ <- initializeBucket(dataprocConfig.leoGoogleBucket, clusterName, bucketName, clusterRequest)
      // Once the bucket is ready, build the cluster
      clusterResponse <- gdDAO.createCluster(googleProject, clusterName, clusterRequest, bucketName)
    } yield {
      clusterResponse
    }
  }

  /* Create a google bucket and populate it with init files */
  private[service] def initializeBucket(googleProject: GoogleProject, clusterName: String, bucketName: String, clusterRequest: ClusterRequest): Future[Unit] = {
    for {
      _ <- gdDAO.createBucket(googleProject, bucketName)
      _ <- initializeBucketObjects(googleProject, clusterName, bucketName, clusterRequest)
    } yield { }
  }

  /* Process the templated cluster init script and put all initialization files in the init bucket */
  private[service] def initializeBucketObjects(googleProject: GoogleProject, clusterName: String, bucketName: String, clusterRequest: ClusterRequest): Future[Unit] = {
    val initScriptPath = dataprocConfig.configFolderPath + dataprocConfig.initActionsScriptName
    val replacements = ClusterInitValues(googleProject, clusterName, bucketName, dataprocConfig, clusterRequest).toJson.asJsObject.fields

    template(initScriptPath, replacements).map { content =>
      val initScriptStorageObject = gdDAO.uploadToBucket(googleProject, bucketName, dataprocConfig.initActionsScriptName, content)
      // Create an array of all the certs, cluster docker compose file, and the site.conf for the apache proxy
      val certs = List(dataprocConfig.jupyterServerCrtName, dataprocConfig.jupyterServerKeyName, dataprocConfig.jupyterRootCaPemName,
        dataprocConfig.clusterDockerComposeName, dataprocConfig.jupyterProxySiteConfName)
      // Put the rest of the initialization files in the init bucket concurrently
      val storageObjects = certs.map { certName => gdDAO.uploadToBucket(googleProject, bucketName, certName, new File(dataprocConfig.configFolderPath, certName)) }
      // Return an array of all the Storage Objects
    }
  }

  /* Process a file using map of replacement values. Each value in the replacement map replaces it's key in the file*/
  private[service] def template(filePath: String, replacementMap: Map[String, JsValue]): Future[String] = {
    Future {
      val raw = scala.io.Source.fromFile(filePath).mkString
      replacementMap.foldLeft(raw)((a, b) => a.replaceAllLiterally("$(" + b._1 + ")", b._2.toString()))
    }
  }

  def getClusterDetails(googleProject: GoogleProject, clusterName: String): Future[Cluster] = {
    dbRef.inTransaction { dataAccess =>
      getCluster(googleProject, clusterName, dataAccess)
    }
  }

  def deleteCluster(googleProject: GoogleProject, clusterName: String): Future[Int] = {
    getClusterDetails(googleProject, clusterName) flatMap { cluster =>
      if(cluster.status.isActive) {
        for {
          _ <- gdDAO.deleteCluster(googleProject, clusterName)
          recordCount <- dbRef.inTransaction(dataAccess => dataAccess.clusterQuery.markPendingDeletion(cluster.googleId))
        } yield {
          clusterMonitorSupervisor ! ClusterDeleted(cluster)
          recordCount
        }
      } else Future.successful(0)
    }
  }
}
