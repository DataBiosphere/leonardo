package org.broadinstitute.dsde.workbench.leonardo.service

import akka.actor.ActorRef
import akka.http.scaladsl.model.StatusCodes
import com.typesafe.scalalogging.LazyLogging
import java.io.File

import org.broadinstitute.dsde.workbench.google.GoogleIamDAO
import org.broadinstitute.dsde.workbench.model.google.{ServiceAccountKey, GoogleProject => WorkbenchGoogleProject}
import org.broadinstitute.dsde.workbench.leonardo.config.{ClusterFilesConfig, ClusterResourcesConfig, DataprocConfig, ProxyConfig, SwaggerConfig}
import org.broadinstitute.dsde.workbench.leonardo.dao.{DataprocDAO, SamDAO}
import org.broadinstitute.dsde.workbench.leonardo.db.{DataAccess, DbReference}
import org.broadinstitute.dsde.workbench.leonardo.model.LeonardoJsonSupport._
import org.broadinstitute.dsde.workbench.leonardo.model.StringValueClass.LabelMap
import org.broadinstitute.dsde.workbench.leonardo.model._
import org.broadinstitute.dsde.workbench.leonardo.monitor.ClusterMonitorSupervisor.{ClusterCreated, ClusterDeleted, RegisterLeoService}
import org.broadinstitute.dsde.workbench.google.gcs._
import org.broadinstitute.dsde.workbench.model.WorkbenchEmail
import slick.dbio.DBIO
import spray.json._

import scala.concurrent.{ExecutionContext, Future}
import scala.io.Source
import scala.util.{Failure, Success}

case class ClusterNotFoundException(googleProject: GoogleProject, clusterName: ClusterName)
  extends LeoException(s"Cluster ${googleProject.string}/${clusterName.string} not found", StatusCodes.NotFound)

case class ClusterAlreadyExistsException(googleProject: GoogleProject, clusterName: ClusterName)
  extends LeoException(s"Cluster ${googleProject.string}/${clusterName.string} already exists", StatusCodes.Conflict)

case class InitializationFileException(googleProject: GoogleProject, clusterName: ClusterName, errorMessage: String)
  extends LeoException(s"Unable to process initialization files for ${googleProject.string}/${clusterName.string}. Returned message: $errorMessage", StatusCodes.Conflict)

case class JupyterExtensionException(gcsUri: GcsPath)
  extends LeoException(s"Jupyter extension URI is invalid or unparseable: ${gcsUri.toUri}", StatusCodes.BadRequest)

case class ParseLabelsException(labelString: String)
  extends LeoException(s"Could not parse label string: $labelString. Expected format [key1=value1,key2=value2,...]", StatusCodes.BadRequest)

case class IllegalLabelKeyException(labelKey: String)
  extends LeoException(s"Labels cannot have a key of '$labelKey'", StatusCodes.NotAcceptable)


class LeonardoService(protected val dataprocConfig: DataprocConfig,
                      protected val clusterFilesConfig: ClusterFilesConfig,
                      protected val clusterResourcesConfig: ClusterResourcesConfig,
                      protected val proxyConfig: ProxyConfig,
                      protected val swaggerConfig: SwaggerConfig,
                      protected val gdDAO: DataprocDAO,
                      protected val googleIamDAO: GoogleIamDAO,
                      protected val dbRef: DbReference,
                      protected val clusterMonitorSupervisor: ActorRef,
                      protected val samDAO: SamDAO)
                     (implicit val executionContext: ExecutionContext) extends LazyLogging {
  private val bucketPathMaxLength = 1024
  private val includeDeletedKey = "includeDeleted"

  // Register this instance with the cluster monitor supervisor so our cluster monitor can potentially delete and recreate clusters
  clusterMonitorSupervisor ! RegisterLeoService(this)

  def createCluster(userInfo: UserInfo, googleProject: GoogleProject, clusterName: ClusterName, clusterRequest: ClusterRequest): Future[Cluster] = {
    samDAO.getPetServiceAccount(userInfo).flatMap { serviceAccount =>
      createCluster(serviceAccount, googleProject, clusterName, clusterRequest)
    }
  }

  def createCluster(serviceAccount: WorkbenchEmail, googleProject: GoogleProject, clusterName: ClusterName, clusterRequest: ClusterRequest): Future[Cluster] = {
    // Check if the google project has a cluster with the same name. If not, we can create it
    dbRef.inTransaction { dataAccess =>
      dataAccess.clusterQuery.getActiveClusterByName(googleProject, clusterName)
    } flatMap {
      case Some(_) => throw ClusterAlreadyExistsException(googleProject, clusterName)
      case None =>
        val augmentedClusterRequest = addClusterDefaultLabels(serviceAccount, googleProject, clusterName, clusterRequest)
        val createFuture = for {
          // Create the cluster in Google
          (cluster, initBucket, serviceAccountKeyOpt) <- createGoogleCluster(serviceAccount, googleProject, clusterName, augmentedClusterRequest)
          // Save the cluster in the database
          savedCluster <- dbRef.inTransaction(_.clusterQuery.save(cluster, GcsPath(initBucket, GcsRelativePath("")), serviceAccountKeyOpt.map(_.id)))
        } yield savedCluster

        createFuture andThen {
          case Success(cluster) =>
            // Notify the cluster monitor upon cluster creation
            clusterMonitorSupervisor ! ClusterCreated(cluster)
        }
    }
  }

  def getActiveClusterDetails(googleProject: GoogleProject, clusterName: ClusterName): Future[Cluster] = {
    dbRef.inTransaction { dataAccess =>
      getActiveCluster(googleProject, clusterName, dataAccess)
    }
  }

  def deleteCluster(googleProject: GoogleProject, clusterName: ClusterName): Future[Int] = {
    getActiveClusterDetails(googleProject, clusterName) flatMap { cluster =>
      if(cluster.status.isActive) {
        // Delete the service account key in Google, if present
        val deleteServiceAccountKey = dbRef.inTransaction { dataAccess =>
          dataAccess.clusterQuery.getServiceAccountKeyId(googleProject, clusterName)
        } flatMap {
          case Some(key) => googleIamDAO.removeServiceAccountKey(WorkbenchGoogleProject(dataprocConfig.leoGoogleProject.string), cluster.googleServiceAccount, key)
          case None => Future.successful(())
        }

        for {
          _ <- deleteServiceAccountKey
          _ <- gdDAO.deleteCluster(googleProject, clusterName)
          recordCount <- dbRef.inTransaction(dataAccess => dataAccess.clusterQuery.markPendingDeletion(cluster.googleId))
        } yield {
          clusterMonitorSupervisor ! ClusterDeleted(cluster)
          recordCount
        }
      } else Future.successful(0)
    }
  }

  def listClusters(params: LabelMap): Future[Seq[Cluster]] = {
   processListClustersParameters(params).flatMap { paramMap =>
     dbRef.inTransaction { dataAccess =>
       dataAccess.clusterQuery.listByLabels(paramMap._1, paramMap._2)
     }
   }
  }

  private[service] def getActiveCluster(googleProject: GoogleProject, clusterName: ClusterName, dataAccess: DataAccess): DBIO[Cluster] = {
    dataAccess.clusterQuery.getActiveClusterByName(googleProject, clusterName) flatMap {
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
  private[service] def createGoogleCluster(serviceAccount: WorkbenchEmail, googleProject: GoogleProject, clusterName: ClusterName, clusterRequest: ClusterRequest)(implicit executionContext: ExecutionContext): Future[(Cluster, GcsBucketName, Option[ServiceAccountKey])] = {
    val initBucketName = generateUniqueBucketName(clusterName.string)
    for {
      // Validate that the Jupyter extension URI is a valid URI and references a real GCS object
      _ <- validateJupyterExtensionUri(googleProject, clusterRequest.jupyterExtensionUri)
      // Create the firewall rule in the google project if it doesn't already exist, so we can access the cluster
      _ <- gdDAO.updateFirewallRule(googleProject)
      // Generate a service account key if configured to do so
      serviceAccountKeyOpt <- generateServiceAccountKey(googleProject, serviceAccount)
      // Add Dataproc Worker role to the pet service account if configured to do so
      _ <- addDataprocWorkerRoleToServiceAccount(googleProject, serviceAccount)
      // Create the bucket in leo's google project and populate with initialization files
      initBucketPath <- initializeBucket(googleProject, clusterName, initBucketName, clusterRequest, serviceAccount, serviceAccountKeyOpt)
      // Once the bucket is ready, build the cluster
      cluster <- gdDAO.createCluster(googleProject, clusterName, clusterRequest, initBucketName, serviceAccount).andThen { case Failure(_) =>
        // If cluster creation fails, delete the init bucket asynchronously
        gdDAO.deleteBucket(googleProject, initBucketName)
      }
    } yield {
      (cluster, initBucketPath, serviceAccountKeyOpt)
    }
  }

  private[service] def generateServiceAccountKey(googleProject: GoogleProject, serviceAccountEmail: WorkbenchEmail): Future[Option[ServiceAccountKey]] = {
    // Only generate a key if NOT creating the cluster as the pet service account.
    // If the pet service account is used to create a cluster, its credentials are on the metadata
    // server and we don't need to propagate a key file.
    if (!dataprocConfig.createClusterAsPetServiceAccount) {
      googleIamDAO.createServiceAccountKey(WorkbenchGoogleProject(dataprocConfig.leoGoogleProject.string), serviceAccountEmail).map(Option(_))
    } else Future.successful(None)
  }

  private[service] def addDataprocWorkerRoleToServiceAccount(googleProject: GoogleProject, serviceAccountEmail: WorkbenchEmail): Future[Unit] = {
    // Only add Dataproc Worker if creating the cluster as the pet service account.
    // Otherwise, the default Google Compute Engine service account is used, which already has the required permissions.
    if (dataprocConfig.createClusterAsPetServiceAccount) {
      googleIamDAO.addIamRolesForUser(WorkbenchGoogleProject(googleProject.string), serviceAccountEmail, Set("roles/dataproc.worker"))
    } else Future.successful(())
  }

  private[service] def validateJupyterExtensionUri(googleProject: GoogleProject, gcsUriOpt: Option[GcsPath])(implicit executionContext: ExecutionContext): Future[Unit] = {
    gcsUriOpt match {
      case None => Future.successful(())
      case Some(gcsPath) =>
        if (gcsPath.toUri.length > bucketPathMaxLength) {
          throw JupyterExtensionException(gcsPath)
        }
        gdDAO.bucketObjectExists(googleProject, gcsPath).map {
          case true => ()
          case false => throw JupyterExtensionException(gcsPath)
        }
    }
  }

  /* Create a google bucket and populate it with init files */
  private[service] def initializeBucket(googleProject: GoogleProject, clusterName: ClusterName, bucketName: GcsBucketName, clusterRequest: ClusterRequest, serviceAccountEmail: WorkbenchEmail, serviceAccountKey: Option[ServiceAccountKey]): Future[GcsBucketName] = {
    for {
      // Note the bucket is created in Leo's project, not the cluster's project.
      // ACLs are granted so the cluster's service account can access the bucket at initialization time.
      _ <- gdDAO.createBucket(dataprocConfig.leoGoogleProject, googleProject, bucketName, serviceAccountEmail)
      _ <- initializeBucketObjects(googleProject, clusterName, bucketName, clusterRequest, serviceAccountKey)
    } yield { bucketName }
  }

  /* Process the templated cluster init script and put all initialization files in the init bucket */
  private[service] def initializeBucketObjects(googleProject: GoogleProject, clusterName: ClusterName, bucketName: GcsBucketName, clusterRequest: ClusterRequest, serviceAccountKey: Option[ServiceAccountKey]): Future[Unit] = {
    // Build a mapping of (name, value) pairs with which to apply templating logic to resources
    val replacements: Map[String, JsValue] = ClusterInitValues(googleProject, clusterName, bucketName, clusterRequest, dataprocConfig,
      clusterFilesConfig, clusterResourcesConfig, proxyConfig, swaggerConfig, serviceAccountKey
    ).toJson.asJsObject.fields

    // Raw files to upload to the bucket, no additional processing needed.
    val filesToUpload = List(
      clusterFilesConfig.jupyterServerCrt,
      clusterFilesConfig.jupyterServerKey,
      clusterFilesConfig.jupyterRootCaPem)

    // Raw resources to upload to the bucket, no additional processing needed.
    // Note: initActionsScript and jupyterGoogleSignInJs are not included
    // because they are post-processed by templating logic.
    val resourcesToUpload = List(
      clusterResourcesConfig.clusterDockerCompose,
      clusterResourcesConfig.jupyterProxySiteConf,
      clusterResourcesConfig.jupyterInstallExtensionScript,
      clusterResourcesConfig.jupyterCustomJs)

    // Uploads the service account private key to the init bucket, if defined.
    // This is a no-op if createClusterAsPetServiceAccount is true.
    val uploadPrivateKeyFuture: Future[Unit] = serviceAccountKey.flatMap(_.privateKeyData.decode).map { k =>
      gdDAO.uploadToBucket(googleProject,
        GcsPath(bucketName, GcsRelativePath(ClusterInitValues.serviceAccountCredentialsFilename)), k)
    } getOrElse(Future.successful(()))

    // Fill in templated resources with the given replacements
    val initScriptContent = templateResource(clusterResourcesConfig.initActionsScript, replacements)
    val googleSignInJsContent = templateResource(clusterResourcesConfig.jupyterGoogleSignInJs, replacements)

    for {
      // Upload the init script to the bucket
      _ <- gdDAO.uploadToBucket(googleProject, GcsPath(bucketName, GcsRelativePath(clusterResourcesConfig.initActionsScript.string)), initScriptContent)

      // Upload the googleSignInJs file to the bucket
      _ <- gdDAO.uploadToBucket(googleProject, GcsPath(bucketName, GcsRelativePath(clusterResourcesConfig.jupyterGoogleSignInJs.string)), googleSignInJsContent)

      // Upload raw files (like certs) to the bucket
      _ <- Future.traverse(filesToUpload)(file => gdDAO.uploadToBucket(googleProject, GcsPath(bucketName, GcsRelativePath(file.getName)), file))

      // Upload raw resources (like cluster-docker-compose.yml, site.conf) to the bucket
      _ <- Future.traverse(resourcesToUpload) { resource =>
        val content = Source.fromResource(s"${ClusterResourcesConfig.basePath}/${resource.string}").mkString
        gdDAO.uploadToBucket(googleProject, GcsPath(bucketName, GcsRelativePath(resource.string)), content)
      }

      // Update the private key json, if defined
      _ <- uploadPrivateKeyFuture
    } yield ()
  }

  /* Process a string using map of replacement values. Each value in the replacement map replaces it's key in the string. */
  private[service] def template(raw: String, replacementMap: Map[String, JsValue]): String = {
    replacementMap.foldLeft(raw)((a, b) => a.replaceAllLiterally("$(" + b._1 + ")", b._2.toString()))
  }

  private[service] def templateFile(file: File, replacementMap: Map[String, JsValue]): String = {
    val raw = Source.fromFile(file).mkString
    template(raw, replacementMap)
  }

  private[service] def templateResource(resource: ClusterResource, replacementMap: Map[String, JsValue]): String = {
    val raw = Source.fromResource(s"${ClusterResourcesConfig.basePath}/${resource.string}").mkString
    template(raw, replacementMap)
  }

  private[service] def processListClustersParameters(params: LabelMap): Future[(LabelMap, Boolean)] = {
    Future {
      params.get(includeDeletedKey) match {
        case Some(includeDeletedValue) => (processLabelMap(params - includeDeletedKey), includeDeletedValue.toBoolean)
        case None => (processLabelMap(params), false)
      }
    }
  }

  /**
    * There are 2 styles of passing labels to the list clusters endpoint:
    *
    * 1. As top-level query string parameters: GET /api/clusters?foo=bar&baz=biz
    * 2. Using the _labels query string parameter: GET /api/clusters?_labels=foo%3Dbar,baz%3Dbiz
    *
    * The latter style exists because Swagger doesn't provide a way to specify free-form query string
    * params. This method handles both styles, and returns a Map[String, String] representing the labels.
    *
    * Note that style 2 takes precedence: if _labels is present on the query string, any additional
    * parameters are ignored.
    *
    * @param params raw query string params
    * @return a Map[String, String] representing the labels
    */
  private[service] def processLabelMap(params: LabelMap): LabelMap = {
    params.get("_labels") match {
      case Some(extraLabels) =>
        extraLabels.split(',').foldLeft(Map.empty[String, String]) { (r, c) =>
          c.split('=') match {
            case Array(key, value) => r + (key -> value)
            case _ => throw ParseLabelsException(extraLabels)
          }
        }
      case None => params
    }
  }

  private[service] def addClusterDefaultLabels(serviceAccount: WorkbenchEmail, googleProject: GoogleProject, clusterName: ClusterName, clusterRequest: ClusterRequest): ClusterRequest = {
    // create a LabelMap of default labels
    val defaultLabels = DefaultLabels(clusterName, googleProject, clusterRequest.bucketPath, serviceAccount, clusterRequest.jupyterExtensionUri)
      .toJson.asJsObject.fields.mapValues(labelValue => labelValue.convertTo[String])
    // combine default and given labels
    val allLabels = clusterRequest.labels ++ defaultLabels
    // check the labels do not contain forbidden keys
    if (allLabels.contains(includeDeletedKey))
      throw IllegalLabelKeyException(includeDeletedKey)
    else clusterRequest.copy(labels = allLabels)
  }
}
