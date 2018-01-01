package org.broadinstitute.dsde.workbench.leonardo.service

import akka.actor.ActorRef
import akka.http.scaladsl.model.StatusCodes
import com.typesafe.scalalogging.LazyLogging
import java.io.File

import cats.data.OptionT
import cats.implicits._
import org.broadinstitute.dsde.workbench.google.GoogleIamDAO
import org.broadinstitute.dsde.workbench.model.google.{GoogleProject, ServiceAccountKey}
import org.broadinstitute.dsde.workbench.leonardo.config.{ClusterFilesConfig, ClusterResourcesConfig, DataprocConfig, ProxyConfig, SwaggerConfig}
import org.broadinstitute.dsde.workbench.leonardo.dao.DataprocDAO
import org.broadinstitute.dsde.workbench.leonardo.db.{DataAccess, DbReference}
import org.broadinstitute.dsde.workbench.leonardo.model.LeonardoJsonSupport._
import org.broadinstitute.dsde.workbench.leonardo.model.StringValueClass.LabelMap
import org.broadinstitute.dsde.workbench.leonardo.model._
import org.broadinstitute.dsde.workbench.leonardo.monitor.ClusterMonitorSupervisor.{ClusterCreated, ClusterDeleted, RegisterLeoService}
import org.broadinstitute.dsde.workbench.google.gcs._
import org.broadinstitute.dsde.workbench.leonardo.model.ProjectActions._
import org.broadinstitute.dsde.workbench.leonardo.model.NotebookClusterActions._
import org.broadinstitute.dsde.workbench.model.{UserInfo, WorkbenchEmail}
import slick.dbio.DBIO
import spray.json._

import scala.concurrent.{ExecutionContext, Future}
import scala.io.Source
import scala.util.control.NonFatal
import scala.util.{Failure, Success}

case class AuthorizationError(email: Option[WorkbenchEmail] = None) extends
  LeoException(s"${email.map(e => s"'${e.value}'").getOrElse("Your account")} is unauthorized", StatusCodes.Unauthorized)

case class ClusterNotFoundException(googleProject: GoogleProject, clusterName: ClusterName)
  extends LeoException(s"Cluster ${googleProject.value}/${clusterName.string} not found", StatusCodes.NotFound)

case class ClusterAlreadyExistsException(googleProject: GoogleProject, clusterName: ClusterName)
  extends LeoException(s"Cluster ${googleProject.value}/${clusterName.string} already exists", StatusCodes.Conflict)

case class InitializationFileException(googleProject: GoogleProject, clusterName: ClusterName, errorMessage: String)
  extends LeoException(s"Unable to process initialization files for ${googleProject.value}/${clusterName.string}. Returned message: $errorMessage", StatusCodes.Conflict)

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
                      protected val authProvider: LeoAuthProvider,
                      protected val serviceAccountProvider: ServiceAccountProvider)
                     (implicit val executionContext: ExecutionContext) extends LazyLogging {
  private val bucketPathMaxLength = 1024
  private val includeDeletedKey = "includeDeleted"

  // Register this instance with the cluster monitor supervisor so our cluster monitor can potentially delete and recreate clusters
  clusterMonitorSupervisor ! RegisterLeoService(this)

  protected def checkProjectPermission(user: UserInfo, action: ProjectAction, project: GoogleProject): Future[Unit] = {
    authProvider.hasProjectPermission(user, action, project.value) map {
      case false => throw AuthorizationError(Option(user.userEmail))
      case true => ()
    }
  }

  //Throws 404 and pretends we don't even know there's a cluster there, by default.
  //If the cluster really exists and you're OK with the user knowing that, set throw401 = true.
  protected def checkClusterPermission(user: UserInfo, action: NotebookClusterAction, cluster: Cluster, throw401: Boolean = false): Future[Unit] = {
    authProvider.hasNotebookClusterPermission(user, action, cluster.googleProject.value, cluster.clusterName.string) map {
      case false =>
        if( throw401 )
          throw AuthorizationError(Option(user.userEmail))
        else
          throw ClusterNotFoundException(cluster.googleProject, cluster.clusterName)
      case true => ()
    }
  }

  def isWhitelisted(userInfo: UserInfo): Future[Unit] = {
    checkProjectPermission(userInfo, ListClusters, GoogleProject("dummy"))
  }

  def createCluster(userInfo: UserInfo, googleProject: GoogleProject, clusterName: ClusterName, clusterRequest: ClusterRequest): Future[Cluster] = {
    for {
      _ <- checkProjectPermission(userInfo, CreateClusters, googleProject)

      // Grab the service accounts from serviceAccountProvider for use later
      clusterServiceAccountOpt <- serviceAccountProvider.getClusterServiceAccount(userInfo, googleProject)
      notebookServiceAccountOpt <- serviceAccountProvider.getNotebookServiceAccount(userInfo, googleProject)
      serviceAccountInfo = ServiceAccountInfo(clusterServiceAccountOpt, notebookServiceAccountOpt)

      cluster <- internalCreateCluster(userInfo.userEmail, serviceAccountInfo, googleProject, clusterName, clusterRequest)
    } yield cluster
  }

  def internalCreateCluster(userEmail: WorkbenchEmail,
                            serviceAccountInfo: ServiceAccountInfo,
                            googleProject: GoogleProject,
                            clusterName: ClusterName,
                            clusterRequest: ClusterRequest): Future[Cluster] = {
    // Check if the google project has a cluster with the same name. If not, we can create it
    dbRef.inTransaction { dataAccess =>
      dataAccess.clusterQuery.getActiveClusterByName(googleProject, clusterName)
    } flatMap {
      case Some(_) => throw ClusterAlreadyExistsException(googleProject, clusterName)
      case None =>
        val augmentedClusterRequest = addClusterDefaultLabels(serviceAccountInfo, googleProject, clusterName, userEmail, clusterRequest)
        val clusterFuture = for {
          // Notify the auth provider that the cluster has been created
          _ <- authProvider.notifyClusterCreated(userEmail.value, googleProject.value, clusterName.string)
          // Create the cluster in Google
          (cluster, initBucket, serviceAccountKeyOpt) <- createGoogleCluster(userEmail, serviceAccountInfo, googleProject, clusterName, augmentedClusterRequest)
          // Save the cluster in the database
          savedCluster <- dbRef.inTransaction(_.clusterQuery.save(cluster, GcsPath(initBucket, GcsRelativePath("")), serviceAccountKeyOpt.map(_.id), clusterRequest.googleClientId))
        } yield {
          // Notify the cluster monitor that the cluster has been created
          clusterMonitorSupervisor ! ClusterCreated(savedCluster)
          savedCluster
        }
        clusterFuture.onComplete {
          case Failure(_) =>
            //make a dummy cluster with the details
            val clusterToDelete = Cluster.createDummyForDeletion(clusterRequest, userEmail, clusterName, googleProject, serviceAccountInfo)
            internalDeleteCluster(clusterToDelete) //don't wait for it
          case Success(_) => //no-op
        }
        clusterFuture
    }
  }

  //throws 404 if nonexistent or no permissions
  def getActiveClusterDetails(userInfo: UserInfo, googleProject: GoogleProject, clusterName: ClusterName): Future[Cluster] = {
    for {
      cluster <- internalGetActiveClusterDetails(googleProject, clusterName) //throws 404 if nonexistent
      _ <- checkClusterPermission(userInfo, GetClusterStatus, cluster) //throws 404 if no auth
    } yield { cluster }
  }

  def internalGetActiveClusterDetails(googleProject: GoogleProject, clusterName: ClusterName): Future[Cluster] = {
    dbRef.inTransaction { dataAccess =>
      getActiveCluster(googleProject, clusterName, dataAccess)
    }
  }

  def deleteCluster(userInfo: UserInfo, googleProject: GoogleProject, clusterName: ClusterName): Future[Unit] = {
    for {
      //throws 404 if no permissions
      cluster <- getActiveClusterDetails(userInfo, googleProject, clusterName)

      //if you've got to here you at least have GetClusterDetails permissions so a 401 is appropriate if you can't actually destroy it
      _ <- checkClusterPermission(userInfo,  DeleteCluster, cluster, throw401 = true)

      _ <- internalDeleteCluster(cluster)
    } yield { () }
  }

  //NOTE: This function MUST ALWAYS complete ALL steps. i.e. if deleting thing1 fails, it must still proceed to delete thing2
  def internalDeleteCluster(cluster: Cluster): Future[Unit] = {
    if (cluster.status.isDeletable) {
      for {
        // Delete the notebook service account key in Google, if present
        _ <- removeServiceAccountKey(cluster.googleProject, cluster.clusterName, cluster.serviceAccountInfo.notebookServiceAccount).recover { case NonFatal(e) =>
          logger.error(s"Error occurred removing service account key for ${cluster.googleProject} / ${cluster.clusterName}", e)
        }
        // Delete the cluster in Google
        _ <- gdDAO.deleteCluster(cluster.googleProject, cluster.clusterName)
        // Change the cluster status to Deleting in the database
        _ <- dbRef.inTransaction(dataAccess => dataAccess.clusterQuery.markPendingDeletion(cluster.googleId))
        // Notify the auth provider of cluster deletion
        _ <- authProvider.notifyClusterDeleted(cluster.creator.value, cluster.googleProject.value, cluster.clusterName.string)
      } yield {
        // Notify the cluster monitor supervisor of cluster deletion.
        // This will kick off polling until the cluster is actually deleted in Google.
        clusterMonitorSupervisor ! ClusterDeleted(cluster)
      }
    } else Future.successful(())
  }

  def listClusters(userInfo: UserInfo, params: LabelMap): Future[Seq[Cluster]] = {
    for {
      paramMap <- processListClustersParameters(params)
      clusterList <- dbRef.inTransaction { da => da.clusterQuery.listByLabels(paramMap._1, paramMap._2) }

      //look up permissions for cluster
      clusterPermissions <- Future.traverse(clusterList) { cluster =>
        val hasProjectPermission = authProvider.hasProjectPermission(userInfo, ListClusters, cluster.googleProject.value)
        val hasNotebookPermission = authProvider.hasNotebookClusterPermission(userInfo, GetClusterStatus, cluster.googleProject.value, cluster.clusterName.string)
        Future.reduceLeft(List(hasProjectPermission, hasNotebookPermission))(_ || _)
      }
    } yield {
      //merge "can we see this cluster" with the cluster and filter out the ones we can't see
      clusterList zip clusterPermissions filter( _._2 ) map ( _._1 )
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
     - Create the cluster in the google project */
  private[service] def createGoogleCluster(userEmail: WorkbenchEmail,
                                           serviceAccountInfo: ServiceAccountInfo,
                                           googleProject: GoogleProject,
                                           clusterName: ClusterName,
                                           clusterRequest: ClusterRequest)
                                          (implicit executionContext: ExecutionContext): Future[(Cluster, GcsBucketName, Option[ServiceAccountKey])] = {
    val initBucketName = generateUniqueBucketName(clusterName.string)
    for {
      // Validate that the Jupyter extension URI is a valid URI and references a real GCS object
      _ <- validateJupyterExtensionUri(googleProject, clusterRequest.jupyterExtensionUri)
      // Create the firewall rule in the google project if it doesn't already exist, so we can access the cluster
      _ <- gdDAO.updateFirewallRule(googleProject)
      // Generate a service account key for the notebook service account (if present) to localize on the cluster.
      // We don't need to do this for the cluster service account because its credentials are already
      // on the metadata server.
      serviceAccountKeyOpt <- generateServiceAccountKey(googleProject, serviceAccountInfo.notebookServiceAccount)
      // Add Dataproc Worker role to the cluster service account, if present.
      // This is needed to be able to spin up Dataproc clusters.
      // If the Google Compute default service account is being used, this is not necessary.
      _ <- addDataprocWorkerRoleToServiceAccount(googleProject, serviceAccountInfo.clusterServiceAccount)
      // Create the bucket in leo's google project and populate with initialization files.
      // ACLs are granted so the cluster service account can access the bucket at initialization time.
      initBucketPath <- initializeBucket(userEmail, googleProject, clusterName, initBucketName, clusterRequest, serviceAccountInfo, serviceAccountKeyOpt)
      // Once the bucket is ready, build the cluster
      cluster <- gdDAO.createCluster(userEmail, googleProject, clusterName, clusterRequest, initBucketName, serviceAccountInfo).andThen { case Failure(_) =>
        // If cluster creation fails, delete the init bucket asynchronously
        gdDAO.deleteBucket(googleProject, initBucketName)
      }
    } yield {
      (cluster, initBucketPath, serviceAccountKeyOpt)
    }
  }

  private[service] def generateServiceAccountKey(googleProject: GoogleProject, serviceAccountOpt: Option[WorkbenchEmail]): Future[Option[ServiceAccountKey]] = {
    serviceAccountOpt.traverse { serviceAccountEmail =>
      googleIamDAO.createServiceAccountKey(dataprocConfig.leoGoogleProject, serviceAccountEmail)
    }
  }

  private[service] def removeServiceAccountKey(googleProject: GoogleProject, clusterName: ClusterName, serviceAccountOpt: Option[WorkbenchEmail]): Future[Unit] = {
    // Delete the service account key in Google, if present
    val tea = for {
      key <- OptionT(dbRef.inTransaction { _.clusterQuery.getServiceAccountKeyId(googleProject, clusterName) })
      serviceAccountEmail <- OptionT.fromOption[Future](serviceAccountOpt)
      _ <- OptionT.liftF(googleIamDAO.removeServiceAccountKey(dataprocConfig.leoGoogleProject, serviceAccountEmail, key))
    } yield ()

    tea.value.void
  }

  private[service] def addDataprocWorkerRoleToServiceAccount(googleProject: GoogleProject, serviceAccountOpt: Option[WorkbenchEmail]): Future[Unit] = {
    serviceAccountOpt.map { serviceAccountEmail =>
      googleIamDAO.addIamRolesForUser(googleProject, serviceAccountEmail, Set("roles/dataproc.worker"))
    } getOrElse Future.successful(())
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
  private[service] def initializeBucket(userEmail: WorkbenchEmail, googleProject: GoogleProject, clusterName: ClusterName, initBucketName: GcsBucketName, clusterRequest: ClusterRequest, serviceAccountInfo: ServiceAccountInfo, notebookServiceAccountKeyOpt: Option[ServiceAccountKey]): Future[GcsBucketName] = {
    for {
      // Note the bucket is created in Leo's project, not the cluster's project.
      // ACLs are granted so the cluster's service account can access the bucket at initialization time.
      _ <- gdDAO.createBucket(dataprocConfig.leoGoogleProject, googleProject, initBucketName, serviceAccountInfo)
      _ <- initializeBucketObjects(userEmail, googleProject, clusterName, initBucketName, clusterRequest, notebookServiceAccountKeyOpt)
    } yield { initBucketName }
  }

  /* Process the templated cluster init script and put all initialization files in the init bucket */
  private[service] def initializeBucketObjects(userEmail: WorkbenchEmail, googleProject: GoogleProject, clusterName: ClusterName, initBucketName: GcsBucketName, clusterRequest: ClusterRequest, serviceAccountKey: Option[ServiceAccountKey]): Future[Unit] = {
    // Build a mapping of (name, value) pairs with which to apply templating logic to resources
    val replacements: Map[String, JsValue] = ClusterInitValues(googleProject, clusterName, initBucketName, clusterRequest, dataprocConfig,
      clusterFilesConfig, clusterResourcesConfig, proxyConfig, swaggerConfig, serviceAccountKey, userEmail
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
        GcsPath(initBucketName, GcsRelativePath(ClusterInitValues.serviceAccountCredentialsFilename)), k)
    } getOrElse(Future.successful(()))

    // Fill in templated resources with the given replacements
    val initScriptContent = templateResource(clusterResourcesConfig.initActionsScript, replacements)
    val googleSignInJsContent = templateResource(clusterResourcesConfig.jupyterGoogleSignInJs, replacements)

    for {
      // Upload the init script to the bucket
      _ <- gdDAO.uploadToBucket(googleProject, GcsPath(initBucketName, GcsRelativePath(clusterResourcesConfig.initActionsScript.string)), initScriptContent)

      // Upload the googleSignInJs file to the bucket
      _ <- gdDAO.uploadToBucket(googleProject, GcsPath(initBucketName, GcsRelativePath(clusterResourcesConfig.jupyterGoogleSignInJs.string)), googleSignInJsContent)

      // Upload raw files (like certs) to the bucket
      _ <- Future.traverse(filesToUpload)(file => gdDAO.uploadToBucket(googleProject, GcsPath(initBucketName, GcsRelativePath(file.getName)), file))

      // Upload raw resources (like cluster-docker-compose.yml, site.conf) to the bucket
      _ <- Future.traverse(resourcesToUpload) { resource =>
        val content = Source.fromResource(s"${ClusterResourcesConfig.basePath}/${resource.string}").mkString
        gdDAO.uploadToBucket(googleProject, GcsPath(initBucketName, GcsRelativePath(resource.string)), content)
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

  private[service] def addClusterDefaultLabels(serviceAccountInfo: ServiceAccountInfo, googleProject: GoogleProject, clusterName: ClusterName, creator: WorkbenchEmail, clusterRequest: ClusterRequest): ClusterRequest = {
    // create a LabelMap of default labels
    val defaultLabels = DefaultLabels(clusterName, googleProject, creator,
      serviceAccountInfo.clusterServiceAccount, serviceAccountInfo.notebookServiceAccount, clusterRequest.jupyterExtensionUri)
      .toJson.asJsObject.fields.mapValues(labelValue => labelValue.convertTo[String])
    // combine default and given labels
    val allLabels = clusterRequest.labels ++ defaultLabels
    // check the labels do not contain forbidden keys
    if (allLabels.contains(includeDeletedKey))
      throw IllegalLabelKeyException(includeDeletedKey)
    else clusterRequest.copy(labels = allLabels)
  }
}
