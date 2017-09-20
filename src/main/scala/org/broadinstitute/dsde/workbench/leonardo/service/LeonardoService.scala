package org.broadinstitute.dsde.workbench.leonardo.service

import java.io.File
import java.net.URI
import java.util.UUID

import akka.actor.ActorRef
import akka.http.scaladsl.model.StatusCodes
import cats.syntax.cartesian._
import cats.instances.option._
import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.dsde.workbench.leonardo.config.DataprocConfig
import org.broadinstitute.dsde.workbench.leonardo.dao._
import org.broadinstitute.dsde.workbench.leonardo.db.{DataAccess, DbReference}
import org.broadinstitute.dsde.workbench.leonardo.model.LeonardoJsonSupport._
import org.broadinstitute.dsde.workbench.leonardo.model.ModelTypes.GoogleProject
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

case class JupyterExtensionException(gcsUri: String)
  extends LeoException(s"Jupyter extension URI is invalid or unparseable: $gcsUri", StatusCodes.BadRequest)

case class ParseLabelsException(labelString: String)
  extends LeoException(s"Could not parse label string: $labelString. Expected format [key1=value1,key2=value2,...]", StatusCodes.BadRequest)

class LeonardoService(protected val dataprocConfig: DataprocConfig, gdDAO: DataprocDAO, dbRef: DbReference, val clusterMonitorSupervisor: ActorRef)(implicit val executionContext: ExecutionContext) extends LazyLogging {
  val bucketPathMaxLength = 1024

  // Register this instance with the cluster monitor supervisor so our cluster monitor can potentially delete and recreate clusters
  clusterMonitorSupervisor ! RegisterLeoService(this)

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

  def listClusters(labelMap: Map[String, String]): Future[Seq[Cluster]] = {
    Future(processLabelMap(labelMap)).flatMap { processedLabelMap =>
      dbRef.inTransaction { dataAccess =>
        dataAccess.clusterQuery.listByLabels(processedLabelMap)
      }
    }
  }

  private[service] def getCluster(googleProject: GoogleProject, clusterName: String, dataAccess: DataAccess): DBIO[Cluster] = {
    dataAccess.clusterQuery.getByName(googleProject, clusterName) flatMap {
      case None => throw ClusterNotFoundException(googleProject, clusterName)
      case Some(cluster) => DBIO.successful(cluster)
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
      // Validate that the Jupyter extension URI is a valid URI and references a real GCS object
      _ <- validateJupyterExtensionUri(googleProject, clusterRequest.jupyterExtensionUri)
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

  private[service] def validateJupyterExtensionUri(googleProject: GoogleProject, gcsUriOpt: Option[String])(implicit executionContext: ExecutionContext): Future[Unit] = {
    gcsUriOpt match {
      case None => Future.successful(())
      case Some(gcsUri) =>
        if (gcsUri.length > bucketPathMaxLength) {
          throw JupyterExtensionException(gcsUri)
        }

        val parsedUri = try { new URI(gcsUri) } catch { case _: Throwable => throw JupyterExtensionException(gcsUri) }

        // URI returns null for host/path if they can't be parsed
        val (bucket, path) = (Option(parsedUri.getHost) |@| Option(parsedUri.getPath)).tupled.getOrElse(throw JupyterExtensionException(gcsUri))

        gdDAO.bucketObjectExists(googleProject, bucket, path.drop(1)).map {
          case true => ()
          case false => throw JupyterExtensionException(gcsUri)
        }
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
    val filesToUpload = List(dataprocConfig.jupyterServerCrtName, dataprocConfig.jupyterServerKeyName, dataprocConfig.jupyterRootCaPemName,
      dataprocConfig.clusterDockerComposeName, dataprocConfig.jupyterProxySiteConfName, dataprocConfig.jupyterInstallExtensionScript)

    for {
      // Fill in templated fields in the init script with the given replacements
      content <- template(initScriptPath, replacements)
      // Upload the init script itself to the bucket
      _ <- gdDAO.uploadToBucket(googleProject, bucketName, dataprocConfig.initActionsScriptName, content)
      // Upload ancillary files like the certs, cluster docker compose file, site.conf, etc to the init bucket
      _ <- Future.traverse(filesToUpload)(name => gdDAO.uploadToBucket(googleProject, bucketName, name, new File(dataprocConfig.configFolderPath, name)))
    } yield ()
  }

  /* Process a file using map of replacement values. Each value in the replacement map replaces it's key in the file*/
  private[service] def template(filePath: String, replacementMap: Map[String, JsValue]): Future[String] = {
    Future {
      val raw = scala.io.Source.fromFile(filePath).mkString
      replacementMap.foldLeft(raw)((a, b) => a.replaceAllLiterally("$(" + b._1 + ")", b._2.toString()))
    }
  }

  private[service] def processLabelMap(params: Map[String, String]): Map[String, String] = {
    // Explode the parameter '_labels=key1=value1,key2=value2' into a Map of keys to values.
    // This is to support swagger which doesn't allow free-form query string parameters.
    params.get("_labels") match {
      case Some(extraLabels) =>
        val extraLabelMap = extraLabels.split(',').foldLeft(Map.empty[String, String]) { (r, c) =>
          c.split('=') match {
            case Array(key, value) => r + (key -> value)
            case _ => throw ParseLabelsException(extraLabels)
          }
        }
        (params - "_labels") ++ extraLabelMap
      case None => params
    }
  }
}
