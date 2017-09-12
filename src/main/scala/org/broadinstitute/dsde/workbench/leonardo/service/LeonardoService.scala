package org.broadinstitute.dsde.workbench.leonardo.service

import akka.http.scaladsl.model.StatusCodes
import com.typesafe.scalalogging.LazyLogging
import java.io.{File, IOException}
import java.util.UUID
import org.broadinstitute.dsde.workbench.leonardo.config.{DataprocConfig}
import org.broadinstitute.dsde.workbench.leonardo.dao._
import org.broadinstitute.dsde.workbench.leonardo.db.{DataAccess, DbReference}
import org.broadinstitute.dsde.workbench.leonardo.model._
import org.broadinstitute.dsde.workbench.leonardo.model.ModelTypes.GoogleProject
import org.broadinstitute.dsde.workbench.leonardo.model.LeonardoJsonSupport._
import scala.concurrent.{ExecutionContext, Future}
import slick.dbio.DBIO
import spray.json._

case class ClusterNotFoundException(googleProject: GoogleProject, clusterName: String)
  extends LeoException(s"Cluster $googleProject/$clusterName not found", StatusCodes.NotFound)

case class ClusterAlreadyExistsException(googleProject: GoogleProject, clusterName: String)
  extends LeoException(s"Cluster $googleProject/$clusterName already exists", StatusCodes.Conflict)

case class InitializationFileException(googleProject: GoogleProject, clusterName: String, errorMessage: String)
  extends LeoException(s"Unable to process initialization files for $googleProject/$clusterName. Returned message: $errorMessage", StatusCodes.Conflict)

case class TemplatingException(filePath: String, errorMessage: String)
  extends LeoException(s"Unable to template file: $filePath. Returned message: $errorMessage", StatusCodes.Conflict)

class LeonardoService(protected val dataprocConfig: DataprocConfig, gdDAO: DataprocDAO, dbRef: DbReference)(implicit val executionContext: ExecutionContext) extends LazyLogging {

  protected def getCluster(googleProject: GoogleProject, clusterName: String, dataAccess: DataAccess): DBIO[Cluster] = {
    dataAccess.clusterQuery.getByName(googleProject, clusterName) flatMap {
      case None => throw ClusterNotFoundException(googleProject, clusterName)
      case Some(cluster) => DBIO.successful(cluster)
    }
  }

  def createCluster(googleProject: String, clusterName: String, clusterRequest: ClusterRequest): Future[Cluster] = {
    def create() = createGoogleCluster(googleProject, clusterName, clusterRequest) flatMap { clusterResponse: ClusterResponse =>
      dbRef.inTransaction { dataAccess =>
        dataAccess.clusterQuery.save(Cluster(clusterRequest, clusterResponse))
      }
    }

    dbRef.inTransaction { dataAccess =>
      dataAccess.clusterQuery.getByName(googleProject, clusterName)
    } flatMap {
      case Some(_) => throw ClusterAlreadyExistsException(googleProject, clusterName)
      case None => create
    }
  }


  /* Creates a cluster in the given google project:
     - Add a firewall rule to the user's google project if it doesn't exist, so we can access the cluster
     - Create the initialization bucket for the cluster in the leo google project
     - Upload all the necessary initialization files to the bucket
     - Create the cluster in the google project
   Currently, the bucketPath of the clusterRequest is not used - it will be used later as a place to store notebook results */
  def createGoogleCluster(googleProject: GoogleProject, clusterName: String, clusterRequest: ClusterRequest)(implicit executionContext: ExecutionContext): Future[ClusterResponse] = {
    val bucketName = s"${clusterName}-${UUID.randomUUID.toString}"
    for {
    // Create the firewall rule in the google project if it doesn't already exist, so we can access the cluster
      _ <- gdDAO.updateFirewallRule(googleProject)
      // Create the bucket in leo's google bucket and populate with initialization files
      _ <- initializeBucket(dataprocConfig.leoGoogleBucket, clusterName, bucketName)
      // Once the bucket is ready, build the cluster
      clusterResponse <- gdDAO.createCluster(googleProject, clusterName, clusterRequest, bucketName)
    } yield {
      clusterResponse
    }
  }

  def initializeBucket(googleProject: GoogleProject, clusterName: String, bucketName: String): Future[Unit] = {
    for {
      bucketResponse <- gdDAO.createBucket(googleProject, bucketName)
      storageObjectsResponse <- initializeBucketObjects(googleProject, clusterName, bucketName)
    } yield {
      storageObjectsResponse
    }
  }

  /* Process the templated cluster init script and put all initialization files in the init bucket */
  def initializeBucketObjects(googleProject: GoogleProject, clusterName: String, bucketName: String): Future[Unit] = {
    val initScriptPath = dataprocConfig.configFolderPath + dataprocConfig.initActionsScriptName
    val replacements = ClusterInitValues(googleProject, clusterName, bucketName, dataprocConfig).toJson.asJsObject.fields

    template(initScriptPath, replacements).map { content =>
      val initScriptStorageObject = gdDAO.uploadToBucket(googleProject, bucketName, dataprocConfig.initActionsScriptName, content)
      // Create an array of all the certs, cluster docker compose file, and the site.conf for the apache proxy
      val certs = List(dataprocConfig.jupyterServerCrtName, dataprocConfig.jupyterServerKeyName, dataprocConfig.jupyterRootCaPemName,
        dataprocConfig.clusterDockerComposeName, dataprocConfig.jupyterProxySiteConfName)
      // Put the rest of the initialization files in the init bucket concurrently
      val storageObjects = certs.map { certName => gdDAO.uploadToBucket(googleProject, bucketName, certName, new File(dataprocConfig.configFolderPath, certName)) }
      // Return an array of all the Storage Objects
    }.recoverWith {
      case ioError: IOException => throw InitializationFileException(googleProject, clusterName, ioError.getMessage)
    }
  }

  def template(filePath: String, replacementMap: Map[String, JsValue]): Future[String] = {
    Future {
      val raw = scala.io.Source.fromFile(filePath).mkString
      replacementMap.foldLeft(raw)((a, b) => a.replaceAllLiterally("$(" + b._1 + ")", b._2.toString()))
    }.recoverWith {
      case t: Throwable => throw TemplatingException(filePath, t.getMessage)
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
