package org.broadinstitute.dsde.workbench.leonardo.service

import java.io.File
import java.time.Instant

import akka.actor.ActorSystem
import akka.http.scaladsl.model.StatusCodes
import cats.Monoid
import cats.data.{Ior, OptionT}
import cats.implicits._
import com.google.api.client.googleapis.json.GoogleJsonResponseException
import com.google.api.client.http.HttpResponseException
import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.dsde.workbench.google.{GoogleIamDAO, GoogleProjectDAO, GoogleStorageDAO}
import org.broadinstitute.dsde.workbench.leonardo._
import org.broadinstitute.dsde.workbench.leonardo.config.{AutoFreezeConfig, ClusterDefaultsConfig, ClusterFilesConfig, ClusterResourcesConfig, DataprocConfig, ProxyConfig, SwaggerConfig}
import org.broadinstitute.dsde.workbench.leonardo.dao.google.{GoogleComputeDAO, GoogleDataprocDAO}
import org.broadinstitute.dsde.workbench.leonardo.db.{DataAccess, DbReference}
import org.broadinstitute.dsde.workbench.leonardo.model.Cluster.LabelMap
import org.broadinstitute.dsde.workbench.leonardo.model.ClusterTool.{Jupyter, RStudio, Welder}
import org.broadinstitute.dsde.workbench.leonardo.model.LeonardoJsonSupport._
import org.broadinstitute.dsde.workbench.leonardo.model.NotebookClusterActions._
import org.broadinstitute.dsde.workbench.leonardo.model.ProjectActions._
import org.broadinstitute.dsde.workbench.leonardo.model._
import org.broadinstitute.dsde.workbench.leonardo.model.google.ClusterStatus.Stopped
import org.broadinstitute.dsde.workbench.leonardo.model.google.DataprocRole._
import org.broadinstitute.dsde.workbench.leonardo.model.google._
import org.broadinstitute.dsde.workbench.leonardo.util.BucketHelper
import org.broadinstitute.dsde.workbench.model.google._
import org.broadinstitute.dsde.workbench.model.{ErrorReport, UserInfo, WorkbenchEmail}
import org.broadinstitute.dsde.workbench.util.Retry
import slick.dbio.DBIO
import spray.json._

import scala.collection.immutable
import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration._
import scala.collection.JavaConverters._
import scala.io.Source
import scala.util.control.NonFatal
import scala.util.{Failure, Success}

case class AuthorizationError(email: Option[WorkbenchEmail] = None)
  extends LeoException(s"${email.map(e => s"'${e.value}'").getOrElse("Your account")} is unauthorized", StatusCodes.Forbidden)

case class AuthenticationError(email: Option[WorkbenchEmail] = None)
  extends LeoException(s"${email.map(e => s"'${e.value}'").getOrElse("Your account")} is not authenticated", StatusCodes.Unauthorized)

case class ClusterNotFoundException(googleProject: GoogleProject, clusterName: ClusterName)
  extends LeoException(s"Cluster ${googleProject.value}/${clusterName.value} not found", StatusCodes.NotFound)

case class ClusterAlreadyExistsException(googleProject: GoogleProject, clusterName: ClusterName, status: ClusterStatus)
  extends LeoException(s"Cluster ${googleProject.value}/${clusterName.value} already exists in ${status.toString} status", StatusCodes.Conflict)

case class ClusterCannotBeStoppedException(googleProject: GoogleProject, clusterName: ClusterName, status: ClusterStatus)
  extends LeoException(s"Cluster ${googleProject.value}/${clusterName.value} cannot be stopped in ${status.toString} status", StatusCodes.Conflict)

case class ClusterCannotBeDeletedException(googleProject: GoogleProject, clusterName: ClusterName)
  extends LeoException(s"Cluster ${googleProject.value}/${clusterName.value} cannot be deleted in Creating status", StatusCodes.Conflict)

case class ClusterCannotBeStartedException(googleProject: GoogleProject, clusterName: ClusterName, status: ClusterStatus)
  extends LeoException(s"Cluster ${googleProject.value}/${clusterName.value} cannot be started in ${status.toString} status", StatusCodes.Conflict)

case class ClusterCannotBeUpdatedException(cluster: Cluster)
  extends LeoException(s"Cluster ${cluster.projectNameString} cannot be updated in ${cluster.status} status", StatusCodes.Conflict)

case class ClusterMachineTypeCannotBeChangedException(cluster: Cluster)
  extends LeoException(s"Cluster ${cluster.projectNameString} in ${cluster.status} status must be stopped in order to change machine type", StatusCodes.Conflict)

case class ClusterDiskSizeCannotBeDecreasedException(cluster: Cluster)
  extends LeoException(s"Cluster ${cluster.projectNameString}: decreasing master disk size is not allowed", StatusCodes.PreconditionFailed)

case class InitializationFileException(googleProject: GoogleProject, clusterName: ClusterName, errorMessage: String)
  extends LeoException(s"Unable to process initialization files for ${googleProject.value}/${clusterName.value}. Returned message: $errorMessage", StatusCodes.Conflict)

case class BucketObjectException(gcsUri: String)
  extends LeoException(s"The provided GCS URI is invalid or unparseable: ${gcsUri}", StatusCodes.BadRequest)

case class BucketObjectAccessException(userEmail: WorkbenchEmail, gcsUri: GcsPath)
  extends LeoException(s"${userEmail.value} does not have access to ${gcsUri.toUri}", StatusCodes.Forbidden)

case class DataprocDisabledException(errorMsg: String)
  extends LeoException(s"${errorMsg}", StatusCodes.Forbidden)

case class ParseLabelsException(labelString: String)
  extends LeoException(s"Could not parse label string: $labelString. Expected format [key1=value1,key2=value2,...]", StatusCodes.BadRequest)

case class IllegalLabelKeyException(labelKey: String)
  extends LeoException(s"Labels cannot have a key of '$labelKey'", StatusCodes.NotAcceptable)

case class InvalidDataprocMachineConfigException(errorMsg: String)
  extends LeoException(s"${errorMsg}", StatusCodes.BadRequest)

class LeonardoService(protected val dataprocConfig: DataprocConfig,
                      protected val clusterFilesConfig: ClusterFilesConfig,
                      protected val clusterResourcesConfig: ClusterResourcesConfig,
                      protected val clusterDefaultsConfig: ClusterDefaultsConfig,
                      protected val proxyConfig: ProxyConfig,
                      protected val swaggerConfig: SwaggerConfig,
                      protected val autoFreezeConfig: AutoFreezeConfig,
                      protected val gdDAO: GoogleDataprocDAO,
                      protected val googleComputeDAO: GoogleComputeDAO,
                      protected val googleIamDAO: GoogleIamDAO,
                      protected val googleProjectDAO: GoogleProjectDAO,
                      protected val leoGoogleStorageDAO: GoogleStorageDAO,
                      protected val petGoogleStorageDAO: String => GoogleStorageDAO,
                      protected val dbRef: DbReference,
                      protected val authProvider: LeoAuthProvider,
                      protected val serviceAccountProvider: ServiceAccountProvider,
                      protected val bucketHelper: BucketHelper,
                      protected val contentSecurityPolicy: String)
                     (implicit val executionContext: ExecutionContext,
                      implicit override val system: ActorSystem) extends LazyLogging with Retry {

  private val bucketPathMaxLength = 1024
  private val includeDeletedKey = "includeDeleted"

  private lazy val firewallRule = FirewallRule(
    name = FirewallRuleName(dataprocConfig.firewallRuleName),
    protocol = FirewallRuleProtocol(proxyConfig.jupyterProtocol),
    ports = List(FirewallRulePort(proxyConfig.jupyterPort.toString)),
    network = dataprocConfig.vpcNetwork.map(VPCNetworkName),
    targetTags = List(NetworkTag(dataprocConfig.networkTag)))

  // Startup script to install on the cluster master node. This allows Jupyter to start back up after
  // a cluster is resumed.
  protected def getMasterInstanceStartupScript(welderEnabled: Boolean): immutable.Map[String, String] = {
    val googleKey = "startup-script"  // required; see https://cloud.google.com/compute/docs/startupscript

    // The || clause is included because older clusters may not have the run-jupyter.sh script installed,
    // so we need to fall back running `jupyter notebook` directly. See https://github.com/DataBiosphere/leonardo/issues/481.
    val jupyterStart = s"docker exec -d ${dataprocConfig.jupyterServerName} /bin/bash -c '/etc/jupyter/scripts/run-jupyter.sh || /usr/local/bin/jupyter notebook'"

    val servicesStart = if (welderEnabled) {
      val welderStart = s"docker exec -u daemon -d ${dataprocConfig.welderServerName} /opt/docker/bin/server start"
      s"($jupyterStart) && $welderStart"
    }
    else jupyterStart

    immutable.Map(googleKey -> servicesStart)
  }

  protected def checkProjectPermission(userInfo: UserInfo, action: ProjectAction, project: GoogleProject): Future[Unit] = {
    authProvider.hasProjectPermission(userInfo, action, project) map {
      case false => throw AuthorizationError(Option(userInfo.userEmail))
      case true => ()
    }
  }

  //Throws 404 and pretends we don't even know there's a cluster there, by default.
  //If the cluster really exists and you're OK with the user knowing that, set throw401 = true.
  protected def checkClusterPermission(userInfo: UserInfo, action: NotebookClusterAction, cluster: Cluster, throw403: Boolean = false): Future[Unit] = {
    authProvider.hasNotebookClusterPermission(userInfo, action, cluster.googleProject, cluster.clusterName) map {
      case false =>
        logger.warn(s"User ${userInfo.userEmail} does not have the notebook permission for " +
          s"${cluster.googleProject}/${cluster.clusterName}")

        if (throw403)
          throw AuthorizationError(Option(userInfo.userEmail))
        else
          throw ClusterNotFoundException(cluster.googleProject, cluster.clusterName)
      case true => ()
    }
  }

  def createCluster(userInfo: UserInfo,
                    googleProject: GoogleProject,
                    clusterName: ClusterName,
                    clusterRequest: ClusterRequest): Future[Cluster] = {
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
    // Check if the google project has an active cluster with the same name. If not, we can create it
    dbRef.inTransaction { dataAccess =>
      dataAccess.clusterQuery.getActiveClusterByName(googleProject, clusterName)
    } flatMap {
      case Some(existingCluster) => throw ClusterAlreadyExistsException(googleProject, clusterName, existingCluster.status)
      case None =>
        val augmentedClusterRequest = augmentClusterRequest(serviceAccountInfo, googleProject, clusterName, userEmail, clusterRequest)
        val clusterImages = processClusterImages(clusterRequest)
        val clusterFuture = for {
          // Notify the auth provider that the cluster has been created
          _ <- authProvider.notifyClusterCreated(userEmail, googleProject, clusterName)

          // Validate that the Jupyter extension URIs and Jupyter user script URI are valid URIs and reference real GCS objects
          _ <- validateClusterRequestBucketObjectUri(userEmail, googleProject, augmentedClusterRequest)

          // Create the cluster in Google
          (cluster, initBucket, serviceAccountKeyOpt) <- createGoogleCluster(userEmail, serviceAccountInfo, googleProject, clusterName, augmentedClusterRequest, clusterImages)

          // Save the cluster in the database
          savedCluster <- dbRef.inTransaction(_.clusterQuery.save(cluster, Option(GcsPath(initBucket, GcsObjectName(""))), serviceAccountKeyOpt.map(_.id)))
        } yield {
          savedCluster
        }

        // If cluster creation failed on the Google side, createGoogleCluster removes resources in Google.
        // We also need to notify our auth provider that the cluster has been deleted.
        clusterFuture.andThen {
          // Don't wait for this future
          case Failure(_) => authProvider.notifyClusterDeleted(userEmail, userEmail, googleProject, clusterName)
        }

        clusterFuture
    }
  }

  // We complete the API response without waiting for the cluster to be created
  // on the Google Dataproc side, which happens asynchronously to the request
  def processClusterCreationRequest(userInfo: UserInfo,
                                    googleProject: GoogleProject,
                                    clusterName: ClusterName,
                                    clusterRequest: ClusterRequest): Future[Cluster] = {
    for {
      _ <- checkProjectPermission(userInfo, CreateClusters, googleProject)

      // Grab the service accounts from serviceAccountProvider for use later
      clusterServiceAccountOpt <- serviceAccountProvider.getClusterServiceAccount(userInfo, googleProject)
      notebookServiceAccountOpt <- serviceAccountProvider.getNotebookServiceAccount(userInfo, googleProject)
      serviceAccountInfo = ServiceAccountInfo(clusterServiceAccountOpt, notebookServiceAccountOpt)

      cluster <- initiateClusterCreation(
        userInfo.userEmail, serviceAccountInfo, googleProject, clusterName, clusterRequest)
    } yield cluster
  }

  // If the google project does not have an active cluster with the given name,
  // we start creating one.
  private def initiateClusterCreation(userEmail: WorkbenchEmail,
                              serviceAccountInfo: ServiceAccountInfo,
                              googleProject: GoogleProject,
                              clusterName: ClusterName,
                              clusterRequest: ClusterRequest): Future[Cluster] = {
    dbRef.inTransaction { dataAccess =>
      dataAccess.clusterQuery.getActiveClusterByNameMinimal(googleProject, clusterName)
    } flatMap {
      case Some(existingCluster) =>
        throw ClusterAlreadyExistsException(googleProject, clusterName, existingCluster.status)
      case None =>
        stageClusterCreation(userEmail, serviceAccountInfo, googleProject, clusterName, clusterRequest)
    }
  }

  private def stageClusterCreation(userEmail: WorkbenchEmail,
                                   serviceAccountInfo: ServiceAccountInfo,
                                   googleProject: GoogleProject,
                                   clusterName: ClusterName,
                                   clusterRequest: ClusterRequest): Future[Cluster] = {

    val augmentedClusterRequest = augmentClusterRequest(serviceAccountInfo, googleProject, clusterName, userEmail, clusterRequest)
    val clusterImages = processClusterImages(clusterRequest)
    val machineConfig = MachineConfigOps.create(clusterRequest.machineConfig, clusterDefaultsConfig)
    val autopauseThreshold = calculateAutopauseThreshold(
      clusterRequest.autopause, clusterRequest.autopauseThreshold)
    val clusterScopes = if(clusterRequest.scopes.isEmpty) dataprocConfig.defaultScopes else clusterRequest.scopes
    val initialClusterToSave = Cluster.create(
      augmentedClusterRequest, userEmail, clusterName, googleProject,
      serviceAccountInfo, machineConfig, dataprocConfig.clusterUrlBase, autopauseThreshold, clusterScopes,
      clusterImages = clusterImages)

    // Validate that the Jupyter extension URIs and Jupyter user script URI are valid URIs and reference real GCS objects
    // and if so, save the cluster creation request parameters in DB
    val attemptToSaveClusterInDb: Future[Cluster] = validateClusterRequestBucketObjectUri(userEmail, googleProject, augmentedClusterRequest)
      .flatMap { _ =>
        logger.info(s"Attempting to notify the AuthProvider for creation of cluster '$clusterName' " +
          s"on Google project '$googleProject'...")
        authProvider.notifyClusterCreated(userEmail, googleProject, clusterName) }
      .flatMap { _ =>
        logger.info(s"Successfully notified the AuthProvider for creation of cluster '$clusterName' " +
          s"on Google project '$googleProject'.")

        dbRef.inTransaction { _.clusterQuery.save(initialClusterToSave) }
      }

    // For the success case, register the following callbacks...
    attemptToSaveClusterInDb foreach { savedInitialCluster =>
      logger.info(s"Inserted an initial record into the DB for cluster '$clusterName' " +
        s"on Google project '$googleProject'.")

      logger.info(s"Attempting to asynchronously create cluster '$clusterName' " +
        s"on Google project '$googleProject'...")

      completeClusterCreation(userEmail, savedInitialCluster, augmentedClusterRequest)
        .onComplete {
          case Success(updatedCluster) =>
            logger.info(s"Successfully submitted to Google the request to create cluster " +
              s"'${updatedCluster.clusterName}' on Google project '${updatedCluster.googleProject}', " +
              s"and updated the database record accordingly. Will monitor the cluster creation process...")
          case Failure(e) =>
            logger.error(s"Failed the asynchronous portion of the creation of cluster '$clusterName' " +
              s"on Google project '$googleProject'.", e)

            // Since we failed, createGoogleCluster removes resources in Google but
            // we also need to notify our auth provider that the cluster has been deleted.
            // We won't wait for that deletion, though.
            authProvider.notifyClusterDeleted(userEmail, userEmail, googleProject, clusterName)

            // We also want to record the error in database for future reference.
            persistErrorInDb(e, clusterName, savedInitialCluster.id, googleProject)
        }
    }

    attemptToSaveClusterInDb
  }

  // Meant to be run asynchronously to the clusterCreate API request
  private def completeClusterCreation(userEmail: WorkbenchEmail,
                                      cluster: Cluster,
                                      clusterRequest: ClusterRequest): Future[Cluster] = {
    logger.info(s"Submitting to Google the request to create cluster '${cluster.clusterName}' " +
      s"on Google project '${cluster.googleProject}'...")

    for {
      (googleCluster, initBucket, serviceAccountKey) <- createGoogleCluster(userEmail, cluster, clusterRequest)

      // We overwrite googleCluster.id with the DB-assigned one that was obtained when we first
      // inserted the record into the DB prior to completing the createCluster request
      googleClusterWithUpdatedId = googleCluster.copy(id = cluster.id)

      _ <- dbRef.inTransaction {
            _.clusterQuery
              .updateAsyncClusterCreationFields(
                Option(GcsPath(initBucket, GcsObjectName(""))), serviceAccountKey, googleClusterWithUpdatedId)
           }
    } yield googleClusterWithUpdatedId
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

  def updateCluster(userInfo: UserInfo,
                    googleProject: GoogleProject,
                    clusterName: ClusterName,
                    clusterRequest: ClusterRequest): Future[Cluster] = {
    for {
      cluster <- internalGetActiveClusterDetails(googleProject, clusterName) //throws 404 if nonexistent

      _ <- checkClusterPermission(userInfo, ModifyCluster, cluster) //throws 404 if no auth

      updatedCluster <- internalUpdateCluster(cluster, clusterRequest)
    } yield { updatedCluster }
  }

  def internalUpdateCluster(existingCluster: Cluster, clusterRequest: ClusterRequest) = {
    implicit val booleanSumMonoidInstance = new Monoid[Boolean] {
      def empty = false
      def combine(a: Boolean, b: Boolean) = a || b
    }

    if (existingCluster.status.isUpdatable) {
      for {
        autopauseChanged <- maybeUpdateAutopauseThreshold(existingCluster, clusterRequest.autopause, clusterRequest.autopauseThreshold).attempt

        clusterResized <- maybeResizeCluster(existingCluster, clusterRequest.machineConfig).attempt

        masterMachineTypeChanged <- maybeChangeMasterMachineType(existingCluster, clusterRequest.machineConfig).attempt

        masterDiskSizeChanged <- maybeChangeMasterDiskSize(existingCluster, clusterRequest.machineConfig).attempt

        // Note: only resizing a cluster triggers a status transition to Updating
        (errors, shouldUpdate) = List(
          autopauseChanged.map(_ => false),
          clusterResized,
          masterMachineTypeChanged.map(_ => false),
          masterDiskSizeChanged.map(_ => false)
        ).separate

        // Set the cluster status to Updating if the cluster was resized
        _ <- if (shouldUpdate.combineAll) {
          dbRef.inTransaction { _.clusterQuery.updateClusterStatus(existingCluster.id, ClusterStatus.Updating) }.void
        } else Future.unit

        cluster <- errors match {
          case Nil => internalGetActiveClusterDetails(existingCluster.googleProject, existingCluster.clusterName)
          // Just return the first error; we don't have a great mechanism to return all errors
          case h :: _ => Future.failed(h)
        }
      } yield cluster

    } else Future.failed(ClusterCannotBeUpdatedException(existingCluster))
  }

  private def getUpdatedValueIfChanged[A](existing: Option[A], updated: Option[A]): Option[A] = {
    (existing, updated) match {
      case (None, Some(0)) => None //An updated value of 0 is considered the same as None to prevent google APIs from complaining
      case (_, Some(x)) if updated != existing => Some(x)
      case _ => None
    }
  }

  def maybeUpdateAutopauseThreshold(existingCluster: Cluster, autopause: Option[Boolean], autopauseThreshold: Option[Int]): Future[Boolean] = {
    val updatedAutopauseThresholdOpt = getUpdatedValueIfChanged(Option(existingCluster.autopauseThreshold), Option(calculateAutopauseThreshold(autopause, autopauseThreshold)))
    updatedAutopauseThresholdOpt match {
      case Some(updatedAutopauseThreshold) =>
        logger.info(s"Changing autopause threshold for cluster ${existingCluster.projectNameString}")

        dbRef.inTransaction { dataAccess =>
          dataAccess.clusterQuery.updateAutopauseThreshold(existingCluster.id, updatedAutopauseThreshold)
        }.as(true)

      case None => Future.successful(false)
    }
  }

  //returns true if cluster was resized, otherwise returns false
  def maybeResizeCluster(existingCluster: Cluster, machineConfigOpt: Option[MachineConfig]): Future[Boolean] = {
    //machineConfig.numberOfPreemtible undefined, and a 0 is passed in
    //
    val updatedNumWorkersAndPreemptiblesOpt = machineConfigOpt.flatMap { machineConfig =>
      Ior.fromOptions(
        getUpdatedValueIfChanged(existingCluster.machineConfig.numberOfWorkers, machineConfig.numberOfWorkers),
        getUpdatedValueIfChanged(existingCluster.machineConfig.numberOfPreemptibleWorkers, machineConfig.numberOfPreemptibleWorkers))
    }

    updatedNumWorkersAndPreemptiblesOpt match {
      case Some(updatedNumWorkersAndPreemptibles) =>
        logger.info(s"New machine config present. Resizing cluster '${existingCluster.projectNameString}'...")

        for {
          // Add Dataproc Worker role to the cluster service account, if present.
          // This is needed to be able to spin up Dataproc clusters.
          // If the Google Compute default service account is being used, this is not necessary.
          _ <- addDataprocWorkerRoleToServiceAccount(existingCluster.googleProject, existingCluster.serviceAccountInfo.clusterServiceAccount)

          // Resize the cluster
          _ <- gdDAO.resizeCluster(existingCluster.googleProject, existingCluster.clusterName, updatedNumWorkersAndPreemptibles.left, updatedNumWorkersAndPreemptibles.right) recoverWith {
            case gjre: GoogleJsonResponseException =>
              //typically we will revoke this role in the monitor after everything is complete, but if Google fails to resize the cluster we need to revoke it manually here
              removeDataprocWorkerRoleFromServiceAccount(existingCluster.googleProject, existingCluster.serviceAccountInfo.clusterServiceAccount)

              logger.info("did not successfully update cluster")
              throw InvalidDataprocMachineConfigException(gjre.getMessage)
          }

          // Update the DB
          _ <- dbRef.inTransaction { dataAccess =>
            updatedNumWorkersAndPreemptibles.fold(
              a => dataAccess.clusterQuery.updateNumberOfWorkers(existingCluster.id, a),
              a => dataAccess.clusterQuery.updateNumberOfPreemptibleWorkers(existingCluster.id, Option(a)),
              (a, b) => dataAccess.clusterQuery.updateNumberOfWorkers(existingCluster.id, a)
                .flatMap(_ =>  dataAccess.clusterQuery.updateNumberOfPreemptibleWorkers(existingCluster.id, Option(b)))
            )
          }
        } yield true

      case None => Future.successful(false)
    }
  }

  def maybeChangeMasterMachineType(existingCluster: Cluster, machineConfigOpt: Option[MachineConfig]): Future[Boolean] = {
    val updatedMasterMachineTypeOpt = machineConfigOpt.flatMap { machineConfig =>
      getUpdatedValueIfChanged(existingCluster.machineConfig.masterMachineType, machineConfig.masterMachineType)
    }

    updatedMasterMachineTypeOpt match {
      // Note: instance must be stopped in order to change machine type
      // TODO future enchancement: add capability to Leo to manage stop/update/restart transitions itself.
      case Some(updatedMasterMachineType) if existingCluster.status == Stopped =>
        logger.info(s"New machine config present. Changing machine type to ${updatedMasterMachineType} for cluster ${existingCluster.projectNameString}...")

        Future.traverse(existingCluster.instances) { instance =>
          instance.dataprocRole match {
            case Some(Master) =>
              googleComputeDAO.setMachineType(instance.key, MachineType(updatedMasterMachineType))
            case _ =>
              // Note: we don't support changing the machine type for worker instances. While this is possible
              // in GCP, Spark settings are auto-tuned to machine size. Dataproc recommends adding or removing nodes,
              // and rebuilding the cluster if new worker machine/disk sizes are needed.
              Future.unit
          }
        }.flatMap { _ =>
          dbRef.inTransaction { _.clusterQuery.updateMasterMachineType(existingCluster.id, MachineType(updatedMasterMachineType)) }
        }.as(true)

      case Some(_) =>
        Future.failed(ClusterMachineTypeCannotBeChangedException(existingCluster))

      case None =>
        Future.successful(false)
    }
  }

  def maybeChangeMasterDiskSize(existingCluster: Cluster, machineConfigOpt: Option[MachineConfig]): Future[Boolean] = {
    val updatedMasterDiskSizeOpt = machineConfigOpt.flatMap { machineConfig =>
      getUpdatedValueIfChanged(existingCluster.machineConfig.masterDiskSize, machineConfig.masterDiskSize)
    }

    // Note: GCE allows you to increase a persistent disk, but not decrease. Throw an exception if the user tries to decrease their disk.
    val diskSizeIncreased = (newSize: Int) => existingCluster.machineConfig.masterDiskSize.exists(_ < newSize)

    updatedMasterDiskSizeOpt match {
      case Some(updatedMasterDiskSize) if diskSizeIncreased(updatedMasterDiskSize) =>
        logger.info(s"New machine config present. Changing master disk size to $updatedMasterDiskSize GB for cluster ${existingCluster.projectNameString}...")

        Future.traverse(existingCluster.instances) { instance =>
          instance.dataprocRole match {
            case Some(Master) =>
              googleComputeDAO.resizeDisk(instance.key, updatedMasterDiskSize)
            case _ =>
              // Note: we don't support changing the machine type for worker instances. While this is possible
              // in GCP, Spark settings are auto-tuned to machine size. Dataproc recommends adding or removing nodes,
              // and rebuilding the cluster if new worker machine/disk sizes are needed.
              Future.unit
          }
        }.flatMap { _ =>
          dbRef.inTransaction { _.clusterQuery.updateMasterDiskSize(existingCluster.id, updatedMasterDiskSize) }
        }.as(true)

      case Some(_) =>
        Future.failed(ClusterDiskSizeCannotBeDecreasedException(existingCluster))

      case None =>
        Future.successful(false)
    }
  }

  def deleteCluster(userInfo: UserInfo, googleProject: GoogleProject, clusterName: ClusterName): Future[Unit] = {
    for {
      //throws 404 if no permissions
      cluster <- getActiveClusterDetails(userInfo, googleProject, clusterName)

      //if you've got to here you at least have GetClusterDetails permissions so a 401 is appropriate if you can't actually destroy it
      _ <- checkClusterPermission(userInfo,  DeleteCluster, cluster, throw403 = true)

      _ <- internalDeleteCluster(userInfo.userEmail, cluster)
    } yield { () }
  }

  //NOTE: This function MUST ALWAYS complete ALL steps. i.e. if deleting thing1 fails, it must still proceed to delete thing2
  def internalDeleteCluster(userEmail: WorkbenchEmail, cluster: Cluster): Future[Unit] = {
    if (cluster.status.isDeletable) {
      for {
        // Delete the notebook service account key in Google, if present
        _ <- removeServiceAccountKey(cluster.googleProject, cluster.clusterName, cluster.serviceAccountInfo.notebookServiceAccount).recover { case NonFatal(e) =>
          logger.error(s"Error occurred removing service account key for ${cluster.googleProject} / ${cluster.clusterName}", e)
        }
        // Delete the cluster in Google
        _ <- gdDAO.deleteCluster(cluster.googleProject, cluster.clusterName)
        // Change the cluster status to Deleting in the database
        // Note this also changes the instance status to Deleting
        _ <- dbRef.inTransaction(dataAccess => dataAccess.clusterQuery.markPendingDeletion(cluster.id))
      } yield { () }
    } else if (cluster.status == ClusterStatus.Creating) {
      Future.failed(ClusterCannotBeDeletedException(cluster.googleProject, cluster.clusterName))
    } else Future.unit
  }

  def stopCluster(userInfo: UserInfo, googleProject: GoogleProject, clusterName: ClusterName): Future[Unit] = {
    for {
      //throws 404 if no permissions
      cluster <- getActiveClusterDetails(userInfo, googleProject, clusterName)

      //if you've got to here you at least have GetClusterDetails permissions so a 401 is appropriate if you can't actually stop it
      _ <- checkClusterPermission(userInfo, StopStartCluster, cluster, throw403 = true)

      _ <- internalStopCluster(cluster)
    } yield ()
  }

  def internalStopCluster(cluster: Cluster): Future[Unit] = {
    if (cluster.status.isStoppable) {
      for {
        // First remove all its preemptible instances in Google, if any
        _ <- if (cluster.machineConfig.numberOfPreemptibleWorkers.exists(_ > 0))
               gdDAO.resizeCluster(cluster.googleProject, cluster.clusterName, numPreemptibles = Some(0))
             else Future.unit

        // Now stop each instance individually
        _ <- Future.traverse(cluster.nonPreemptibleInstances) { instance =>
          // Install a startup script on the master node so Jupyter starts back up again once the instance is restarted
          instance.dataprocRole match {
            case Some(Master) =>
              if (cluster.clusterImages.map(_.tool) contains (Jupyter)) {
                googleComputeDAO.addInstanceMetadata(instance.key, getMasterInstanceStartupScript(cluster.welderEnabled)).flatMap { _ =>
                  googleComputeDAO.stopInstance(instance.key)
                }
              }
              else googleComputeDAO.stopInstance(instance.key)
            case _ =>
              googleComputeDAO.stopInstance(instance.key)
          }
        }

        // Update the cluster status to Stopping
        _ <- dbRef.inTransaction { _.clusterQuery.setToStopping(cluster.id) }
      } yield { }

    } else Future.failed(ClusterCannotBeStoppedException(cluster.googleProject, cluster.clusterName, cluster.status))
  }

  def startCluster(userInfo: UserInfo, googleProject: GoogleProject, clusterName: ClusterName): Future[Unit] = {
    for {
      //throws 404 if no permissions
      cluster <- getActiveClusterDetails(userInfo, googleProject, clusterName)

      //if you've got to here you at least have GetClusterDetails permissions so a 401 is appropriate if you can't actually stop it
      _ <- checkClusterPermission(userInfo, StopStartCluster, cluster, throw403 = true)

      _ <- internalStartCluster(userInfo.userEmail, cluster)
    } yield ()
  }

  def internalStartCluster(userEmail: WorkbenchEmail, cluster: Cluster): Future[Unit] = {
    if (cluster.status.isStartable) {
      for {
        // Add back the preemptible instances
        _ <- if (cluster.machineConfig.numberOfPreemptibleWorkers.exists(_ > 0))
               gdDAO.resizeCluster(cluster.googleProject, cluster.clusterName, numPreemptibles = cluster.machineConfig.numberOfPreemptibleWorkers)
             else Future.unit

        // Start each instance individually
        _ <- Future.traverse(cluster.nonPreemptibleInstances) { instance =>
          googleComputeDAO.startInstance(instance.key)
        }

        // Update the cluster status to Starting
        _ <- dbRef.inTransaction { _.clusterQuery.updateClusterStatus(cluster.id, ClusterStatus.Starting) }
      } yield { () }

    } else Future.failed(ClusterCannotBeStartedException(cluster.googleProject, cluster.clusterName, cluster.status))
  }

  def listClusters(userInfo: UserInfo, params: LabelMap, googleProjectOpt: Option[GoogleProject] = None): Future[Seq[Cluster]] = {
    for {
      paramMap <- processListClustersParameters(params)
      clusterList <- dbRef.inTransaction { da => da.clusterQuery.listByLabels(paramMap._1, paramMap._2, googleProjectOpt) }
      samVisibleClusters <- authProvider.filterUserVisibleClusters(userInfo, clusterList.map(c => (c.googleProject, c.clusterName)).toList)
    } yield {
      // Making the assumption that users will always be able to access clusters that they create
      // Fix for https://github.com/DataBiosphere/leonardo/issues/821
      val visibleClusters = samVisibleClusters:::clusterList.filter(_.auditInfo.creator == userInfo.userEmail).map(c => (c.googleProject, c.clusterName)).toList
      val visibleClustersSet = visibleClusters.toSet
      clusterList.filter(c => visibleClustersSet.contains((c.googleProject, c.clusterName)))
    }
  }

  private[service] def getActiveCluster(googleProject: GoogleProject, clusterName: ClusterName, dataAccess: DataAccess): DBIO[Cluster] = {
    dataAccess.clusterQuery.getActiveClusterByName(googleProject, clusterName) flatMap {
      case None => throw ClusterNotFoundException(googleProject, clusterName)
      case Some(cluster) => DBIO.successful(cluster)
    }
  }

  private[service] def createGoogleCluster(userEmail: WorkbenchEmail,
                                           cluster: Cluster,
                                           clusterRequest: ClusterRequest)
                                          (implicit executionContext: ExecutionContext): Future[(Cluster, GcsBucketName, Option[ServiceAccountKey])] = {
    createGoogleCluster(userEmail, cluster.serviceAccountInfo, cluster.googleProject, cluster.clusterName, clusterRequest, cluster.clusterImages)
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
                                           clusterRequest: ClusterRequest,
                                           clusterImages: Set[ClusterImage])
                                          (implicit executionContext: ExecutionContext): Future[(Cluster, GcsBucketName, Option[ServiceAccountKey])] = {
    val initBucketName = generateUniqueBucketName("leoinit-"+clusterName.value)
    val stagingBucketName = generateUniqueBucketName("leostaging-"+clusterName.value)

    val googleFuture = for {
      // Create the firewall rule in the google project if it doesn't already exist, so we can access the cluster
      _ <- googleComputeDAO.updateFirewallRule(googleProject, firewallRule)

      // Generate a service account key for the notebook service account (if present) to localize on the cluster.
      // We don't need to do this for the cluster service account because its credentials are already
      // on the metadata server.
      serviceAccountKeyOpt <- generateServiceAccountKey(googleProject, serviceAccountInfo.notebookServiceAccount)

      // Add Dataproc Worker role to the cluster service account, if present.
      // This is needed to be able to spin up Dataproc clusters.
      // If the Google Compute default service account is being used, this is not necessary.
      _ <- addDataprocWorkerRoleToServiceAccount(googleProject, serviceAccountInfo.clusterServiceAccount)

      // Create the bucket in the cluster's google project and populate with initialization files.
      // ACLs are granted so the cluster service account can access the files at initialization time.
      initBucket <- bucketHelper.createInitBucket(googleProject, initBucketName, serviceAccountInfo)
      _ <- initializeBucketObjects(userEmail, googleProject, clusterName, initBucket, clusterRequest, serviceAccountKeyOpt, contentSecurityPolicy, clusterImages, stagingBucketName)

      // Create the cluster staging bucket. ACLs are granted so the user/pet can access it.
      stagingBucket <- bucketHelper.createStagingBucket(userEmail, googleProject, stagingBucketName, serviceAccountInfo)

      // build cluster configuration
      machineConfig = MachineConfigOps.create(clusterRequest.machineConfig, clusterDefaultsConfig)
      initScript = GcsPath(initBucket, GcsObjectName(clusterResourcesConfig.initActionsScript.value))
      autopauseThreshold = calculateAutopauseThreshold(clusterRequest.autopause, clusterRequest.autopauseThreshold)
      clusterScopes = if(clusterRequest.scopes.isEmpty) dataprocConfig.defaultScopes else clusterRequest.scopes
      credentialsFileName = serviceAccountInfo.notebookServiceAccount.map(_ => s"/etc/${ClusterInitValues.serviceAccountCredentialsFilename}")

      // decide whether to use VPC network
      lookupProjectLabels = dataprocConfig.projectVPCNetworkLabel.isDefined || dataprocConfig.projectVPCSubnetLabel.isDefined
      projectLabels <- if (lookupProjectLabels) googleProjectDAO.getLabels(googleProject.value) else Future.successful(Map.empty[String, String])
      clusterVPCSettings = getClusterVPCSettings(projectLabels)

      // Create the cluster
      createClusterConfig = CreateClusterConfig(machineConfig, initScript, serviceAccountInfo.clusterServiceAccount, credentialsFileName, stagingBucket, clusterScopes, clusterVPCSettings, clusterRequest.properties)
      retryResult <- retryExponentially(whenGoogleZoneCapacityIssue, "Cluster creation failed because zone with adequate resources was not found") { () =>
        gdDAO.createCluster(googleProject, clusterName, createClusterConfig)
      }
      operation <- retryResult match {
        case Right((_, op)) => Future.successful(op)
        case Left(errors) =>
          Metrics.newRelic.incrementCounterIO("zoneCapacityClusterCreationFailure").unsafeRunAsync(_ => ())
          val exceptionMessage = errors.toList.map(_.toString).mkString(", ")
          Future.failed(new Exception(exceptionMessage))
      }
      cluster = Cluster.create(clusterRequest, userEmail, clusterName, googleProject, serviceAccountInfo,
        machineConfig, dataprocConfig.clusterUrlBase, autopauseThreshold, clusterScopes, Some(operation), Option(stagingBucket), clusterImages)
    } yield (cluster, initBucket, serviceAccountKeyOpt)

    // If anything fails, we need to clean up Google resources that might have been created
    googleFuture.andThen { case Failure(t) =>
      // Don't wait for this future
      cleanUpGoogleResourcesOnError(t, googleProject, clusterName, initBucketName, serviceAccountInfo)
    }
  }

  private def whenGoogleZoneCapacityIssue(throwable: Throwable): Boolean = {
    throwable match {
      case t: GoogleJsonResponseException => t.getStatusCode == 429 && t.getDetails.getErrors.asScala.head.getReason.equalsIgnoreCase("rateLimitExceeded")
      case _ => false
    }
  }

  def getClusterVPCSettings(projectLabels: Map[String, String]): Option[Either[VPCNetworkName, VPCSubnetName]] = {
    //Dataproc only allows you to specify a subnet OR a network. Subnets will be preferred if present.
    //High-security networks specified inside of the project will always take precedence over anything
    //else. Thus, VPC configuration takes the following precedence:
    // 1) High-security subnet in the project (if present)
    // 2) High-security network in the project (if present)
    // 3) Subnet specified in leonardo.conf (if present)
    // 4) Network specified in leonardo.conf (if present)
    // 5) The default network in the project
    val projectSubnet  = dataprocConfig.projectVPCSubnetLabel.flatMap(subnetLabel => projectLabels.get(subnetLabel).map(VPCSubnetName) )
    val projectNetwork = dataprocConfig.projectVPCNetworkLabel.flatMap( networkLabel => projectLabels.get(networkLabel).map(VPCNetworkName) )
    val configSubnet   = dataprocConfig.vpcSubnet.map(VPCSubnetName)
    val configNetwork  = dataprocConfig.vpcNetwork.map(VPCNetworkName)

    (projectSubnet, projectNetwork, configSubnet, configNetwork) match {
      case (Some(subnet), _, _, _)  => Some(Right(subnet))
      case (_, Some(network), _, _) => Some(Left(network))
      case (_, _, Some(subnet), _)  => Some(Right(subnet))
      case (_, _, _, Some(network)) => Some(Left(network))
      case (_, _, _, _)             => None
    }
  }

  private def calculateAutopauseThreshold(autopause: Option[Boolean], autopauseThreshold: Option[Int]): Int = {
    autopause match {
      case None =>
        autoFreezeConfig.autoFreezeAfter.toMinutes.toInt
      case Some(false) =>
        autoPauseOffValue
      case _ =>
        if (autopauseThreshold.isEmpty) autoFreezeConfig.autoFreezeAfter.toMinutes.toInt
        else Math.max(autoPauseOffValue, autopauseThreshold.get)
    }
  }

  private def persistErrorInDb(e: Throwable,
                               clusterName: ClusterName,
                               clusterId: Long,
                               googleProject: GoogleProject): Future[Unit] = {
    val errorMessage = e match {
      case leoEx: LeoException =>
        ErrorReport.loggableString(leoEx.toErrorReport)
      case _ =>
        s"Asynchronous creation of cluster '$clusterName' on Google project " +
          s"'$googleProject' failed due to '${e.toString}'."
    }

    // TODO Make errorCode field nullable in ClusterErrorComponent and pass None below
    // See https://github.com/DataBiosphere/leonardo/issues/512
    val dummyErrorCode = -1

    val errorInfo = ClusterError(errorMessage, dummyErrorCode, Instant.now)

    dbRef.inTransaction { dataAccess =>
      for {
        _ <- dataAccess.clusterQuery.updateClusterStatus(clusterId, ClusterStatus.Error)
        _ <- dataAccess.clusterErrorQuery.save(clusterId, errorInfo)
      } yield ()
    }
  }

  private[service] def cleanUpGoogleResourcesOnError(throwable: Throwable, googleProject: GoogleProject, clusterName: ClusterName, initBucketName: GcsBucketName, serviceAccountInfo: ServiceAccountInfo): Future[Unit] = {
    logger.error(s"Cluster creation failed in Google for $googleProject / ${clusterName.value}. Cleaning up resources in Google...")

    // Clean up resources in Google

    val deleteInitBucketFuture = leoGoogleStorageDAO.deleteBucket(initBucketName, recurse = true) map { _ =>
      logger.info(s"Successfully deleted init bucket ${initBucketName.value} for  ${googleProject.value} / ${clusterName.value}")
    } recover { case e =>
      logger.error(s"Failed to delete init bucket ${initBucketName.value} for  ${googleProject.value} / ${clusterName.value}", e)
    }

    // Don't delete the staging bucket so the user can see error logs.

    val deleteClusterFuture = gdDAO.deleteCluster(googleProject, clusterName) map { _ =>
      logger.info(s"Successfully deleted cluster ${googleProject.value} / ${clusterName.value}")
    } recover { case e =>
      logger.error(s"Failed to delete cluster ${googleProject.value} / ${clusterName.value}", e)
    }

    val deleteServiceAccountKeyFuture =  removeServiceAccountKey(googleProject, clusterName, serviceAccountInfo.notebookServiceAccount) map { _ =>
      logger.info(s"Successfully deleted service account key for ${serviceAccountInfo.notebookServiceAccount}")
    } recover { case e =>
      logger.error(s"Failed to delete service account key for ${serviceAccountInfo.notebookServiceAccount}", e)
    }

    Future.sequence(Seq(deleteInitBucketFuture, deleteClusterFuture, deleteServiceAccountKeyFuture)).void
  }

  private[service] def generateServiceAccountKey(googleProject: GoogleProject, serviceAccountOpt: Option[WorkbenchEmail]): Future[Option[ServiceAccountKey]] = {
    serviceAccountOpt.traverse { serviceAccountEmail =>
      googleIamDAO.createServiceAccountKey(googleProject, serviceAccountEmail)
    }
  }

  private[service] def removeServiceAccountKey(googleProject: GoogleProject, clusterName: ClusterName, serviceAccountOpt: Option[WorkbenchEmail]): Future[Unit] = {
    // Delete the service account key in Google, if present
    val tea = for {
      key <- OptionT(dbRef.inTransaction { _.clusterQuery.getServiceAccountKeyId(googleProject, clusterName) })
      serviceAccountEmail <- OptionT.fromOption[Future](serviceAccountOpt)
      _ <- OptionT.liftF(googleIamDAO.removeServiceAccountKey(googleProject, serviceAccountEmail, key))
    } yield ()

    tea.value.void
  }

  private def whenGoogle409(throwable: Throwable): Boolean = {
    throwable match {
      case t: GoogleJsonResponseException => t.getStatusCode == 409
      case _ => false
    }
  }

  private[service] def addDataprocWorkerRoleToServiceAccount(googleProject: GoogleProject, serviceAccountOpt: Option[WorkbenchEmail]): Future[Unit] = {
    serviceAccountOpt.map { serviceAccountEmail =>
      // Retry 409s with exponential backoff. This can happen if concurrent policy updates are made in the same project.
      // Google recommends a retry in this case.
      val iamFuture: Future[Unit] = retryExponentially(whenGoogle409, s"IAM policy change failed for Google project '$googleProject'") { () =>
        googleIamDAO.addIamRolesForUser(googleProject, serviceAccountEmail, Set("roles/dataproc.worker"))
      }
      iamFuture
    } getOrElse Future.unit
  }

  private[service] def removeDataprocWorkerRoleFromServiceAccount(googleProject: GoogleProject, serviceAccountOpt: Option[WorkbenchEmail]): Future[Unit] = {
    serviceAccountOpt.map { serviceAccountEmail =>
      // Retry 409s with exponential backoff. This can happen if concurrent policy updates are made in the same project.
      // Google recommends a retry in this case.
      val iamFuture: Future[Unit] = retryExponentially(whenGoogle409, s"IAM policy change failed for Google project '$googleProject'") { () =>
        googleIamDAO.removeIamRolesForUser(googleProject, serviceAccountEmail, Set("roles/dataproc.worker"))
      }
      iamFuture
    } getOrElse Future.unit
  }

  private def validateClusterRequestBucketObjectUri(userEmail: WorkbenchEmail, googleProject: GoogleProject, clusterRequest: ClusterRequest)
                                                   (implicit executionContext: ExecutionContext): Future[Unit] = {
    val transformed = for {
      // Get a pet token from Sam. If we can't get a token, we won't do validation but won't fail cluster creation.
      petToken <- OptionT(serviceAccountProvider.getAccessToken(userEmail, googleProject).recover { case e =>
        logger.warn(s"Could not acquire pet service account access token for user ${userEmail.value} in project $googleProject. " +
          s"Skipping validation of bucket objects in the cluster request.", e)
        None
      })

      // Validate the user script URI
      _ <- clusterRequest.jupyterUserScriptUri match {
        case Some(userScriptUri) => OptionT.liftF[Future, Unit](validateBucketObjectUri(userEmail, petToken, userScriptUri.toUri))
        case None => OptionT.pure[Future](())
      }

      // Validate the extension URIs
      _ <- clusterRequest.userJupyterExtensionConfig match {
        case Some(config) =>
          val extensionsToValidate = (config.nbExtensions.values ++ config.serverExtensions.values ++ config.combinedExtensions.values).filter(_.startsWith("gs://"))
          OptionT.liftF(Future.traverse(extensionsToValidate)(x => validateBucketObjectUri(userEmail, petToken, x)))
        case None => OptionT.pure[Future](())
      }
    } yield ()

    // Because of how OptionT works, `transformed.value` returns a Future[Option[Unit]]. `void` converts this to a Future[Unit].
    transformed.value.void
  }

  private[service] def validateBucketObjectUri(userEmail: WorkbenchEmail, userToken: String, gcsUri: String)
                                              (implicit executionContext: ExecutionContext): Future[Unit] = {
    logger.debug(s"Validating user [${userEmail.value}] has access to bucket object $gcsUri")
    val gcsUriOpt = parseGcsPath(gcsUri)
    gcsUriOpt match {
      case Left(_) => Future.failed(BucketObjectException(gcsUri))
      case Right(gcsPath) if gcsPath.toUri.length > bucketPathMaxLength => Future.failed(BucketObjectException(gcsUri))
      case Right(gcsPath) =>
        // Retry 401s from Google here because they can be thrown spuriously with valid credentials.
        // See https://github.com/DataBiosphere/leonardo/issues/460
        // Note GoogleStorageDAO already retries 500 and other errors internally, so we just need to catch 401s here.
        // We might think about moving the retry-on-401 logic inside GoogleStorageDAO.
        val errorMessage = s"GCS object validation failed for user [${userEmail.value}] and token [$userToken] and object [${gcsUri}]"
        val gcsFuture: Future[Boolean] = retryUntilSuccessOrTimeout(whenGoogle401, errorMessage)(interval = 1 second, timeout = 3 seconds) { () =>
          petGoogleStorageDAO(userToken).objectExists(gcsPath.bucketName, gcsPath.objectName)
        }
        gcsFuture.map {
          case true => ()
          case false => throw BucketObjectException(gcsPath.toUri)
        } recover {
          case e: HttpResponseException if e.getStatusCode == StatusCodes.Forbidden.intValue =>
            logger.error(s"User ${userEmail.value} does not have access to ${gcsPath.bucketName} / ${gcsPath.objectName}")
            throw BucketObjectAccessException(userEmail, gcsPath)
          case e if whenGoogle401(e) =>
            logger.warn(s"Could not validate object [${gcsUri}] as user [${userEmail.value}]", e)
            ()
        }
    }
  }

  private def whenGoogle401(t: Throwable): Boolean = t match {
    case g: GoogleJsonResponseException if g.getStatusCode == StatusCodes.Unauthorized.intValue => true
    case _ => false
  }

  /* Process the templated cluster init script and put all initialization files in the init bucket */
  private[service] def initializeBucketObjects(userEmail: WorkbenchEmail,
                                               googleProject: GoogleProject,
                                               clusterName: ClusterName,
                                               initBucketName: GcsBucketName,
                                               clusterRequest: ClusterRequest,
                                               serviceAccountKey: Option[ServiceAccountKey],
                                               contentSecurityPolicy: String,
                                               clusterImages: Set[ClusterImage],
                                               stagingBucket: GcsBucketName): Future[Unit] = {

    // Build a mapping of (name, value) pairs with which to apply templating logic to resources
    val clusterInit = ClusterInitValues(googleProject, clusterName, initBucketName, clusterRequest, dataprocConfig,
      clusterFilesConfig, clusterResourcesConfig, proxyConfig, serviceAccountKey, userEmail, contentSecurityPolicy, clusterImages, stagingBucket)
    val replacements: Map[String, String] = clusterInit.toMap

    // Raw files to upload to the bucket, no additional processing needed.
    val filesToUpload = List(
      clusterFilesConfig.jupyterServerCrt,
      clusterFilesConfig.jupyterServerKey,
      clusterFilesConfig.jupyterRootCaPem)

    // Raw resources to upload to the bucket, no additional processing needed.
    // Note: initActionsScript and jupyterGoogleSignInJs are not included
    // because they are post-processed by templating logic.
    val resourcesToUpload = List(
      clusterResourcesConfig.jupyterDockerCompose,
      clusterResourcesConfig.rstudioDockerCompose,
      clusterResourcesConfig.proxyDockerCompose,
      clusterResourcesConfig.proxySiteConf,
      clusterResourcesConfig.extensionEntry,
      clusterResourcesConfig.jupyterLabGooglePlugin,
      clusterResourcesConfig.welderDockerCompose
    )

    // Uploads the service account private key to the init bucket, if defined.
    // This is a no-op if createClusterAsPetServiceAccount is true.
    val uploadPrivateKeyFuture: Future[Unit] = serviceAccountKey.flatMap(_.privateKeyData.decode).map { k =>
      leoGoogleStorageDAO.storeObject(initBucketName, GcsObjectName(ClusterInitValues.serviceAccountCredentialsFilename), k, "text/plain")
    } getOrElse(Future.unit)

    // Fill in templated resources with the given replacements
    val initScriptContent = templateResource(clusterResourcesConfig.initActionsScript, replacements)
    val googleSignInJsContent = templateResource(clusterResourcesConfig.googleSignInJs, replacements)
    val editModeJsContent = templateResource(clusterResourcesConfig.editModeJs, replacements)
    val safeModeJsContent = templateResource(clusterResourcesConfig.safeModeJs, replacements)
    val jupyterNotebookConfigContent = templateResource(clusterResourcesConfig.jupyterNotebookConfigUri, replacements)

    for {
      // Upload the init script to the bucket
      _ <- leoGoogleStorageDAO.storeObject(initBucketName, GcsObjectName(clusterResourcesConfig.initActionsScript.value), initScriptContent, "text/plain")

      // Upload the nb extensions to the bucket
      _ <- leoGoogleStorageDAO.storeObject(initBucketName, GcsObjectName(clusterResourcesConfig.googleSignInJs.value), googleSignInJsContent, "text/plain")
      _ <- leoGoogleStorageDAO.storeObject(initBucketName, GcsObjectName(clusterResourcesConfig.editModeJs.value), editModeJsContent, "text/plain")
      _ <- leoGoogleStorageDAO.storeObject(initBucketName, GcsObjectName(clusterResourcesConfig.safeModeJs.value), safeModeJsContent, "text/plain")


      // Update the jupytyer notebook config file
      _ <- leoGoogleStorageDAO.storeObject(initBucketName, GcsObjectName(clusterResourcesConfig.jupyterNotebookConfigUri.value), jupyterNotebookConfigContent, "text/plain")

      // Upload raw files (like certs) to the bucket
      _ <- Future.traverse(filesToUpload)(file => leoGoogleStorageDAO.storeObject(initBucketName, GcsObjectName(file.getName), file, "text/plain"))

      // Upload raw resources (like cluster-docker-compose.yml, site.conf) to the bucket
      _ <- Future.traverse(resourcesToUpload) { resource =>
        val content = Source.fromResource(s"${ClusterResourcesConfig.basePath}/${resource.value}").mkString
        leoGoogleStorageDAO.storeObject(initBucketName, GcsObjectName(resource.value), content, "text/plain")
      }

      // Update the private key json, if defined
      _ <- uploadPrivateKeyFuture
    } yield ()
  }

  // Process a string using map of replacement values. Each value in the replacement map replaces its key in the string.
  private[service] def template(raw: String, replacementMap: Map[String, String]): String = {
    replacementMap.foldLeft(raw)((a, b) => a.replaceAllLiterally("$(" + b._1 + ")", "\"" + b._2 + "\""))
  }

  private[service] def templateFile(file: File, replacementMap: Map[String, String]): String = {
    val raw = Source.fromFile(file).mkString
    template(raw, replacementMap)
  }

  private[service] def templateResource(resource: ClusterResource, replacementMap: Map[String, String]): String = {
    val raw = Source.fromResource(s"${ClusterResourcesConfig.basePath}/${resource.value}").mkString
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

  private[service] def augmentClusterRequest(serviceAccountInfo: ServiceAccountInfo, googleProject: GoogleProject, clusterName: ClusterName, userEmail: WorkbenchEmail, clusterRequest: ClusterRequest) = {
    val userJupyterExt = clusterRequest.jupyterExtensionUri match {
      case Some(ext) => Map[String, String]("notebookExtension" -> ext.toUri)
      case None => Map[String, String]()
    }

    // add the userJupyterExt to the nbExtensions
    val updatedUserJupyterExtensionConfig = clusterRequest.userJupyterExtensionConfig match {
      case Some(config) => config.copy(nbExtensions = config.nbExtensions ++ userJupyterExt)
      case None => UserJupyterExtensionConfig(userJupyterExt, Map.empty, Map.empty, Map.empty)
    }

    // transform Some(empty, empty, empty, empty) to None
    // TODO: is this really necessary?
    val updatedClusterRequest = clusterRequest.copy(userJupyterExtensionConfig = if (updatedUserJupyterExtensionConfig.asLabels.isEmpty)
      None
    else
      Some(updatedUserJupyterExtensionConfig))

    addClusterLabels(serviceAccountInfo, googleProject, clusterName, userEmail, updatedClusterRequest)
  }

  private[service] def addClusterLabels(serviceAccountInfo: ServiceAccountInfo,
                                        googleProject: GoogleProject,
                                        clusterName: ClusterName,
                                        creator: WorkbenchEmail,
                                        clusterRequest: ClusterRequest): ClusterRequest = {
    // create a LabelMap of default labels
    val defaultLabels = DefaultLabels(clusterName, googleProject, creator,
      serviceAccountInfo.clusterServiceAccount, serviceAccountInfo.notebookServiceAccount, clusterRequest.jupyterUserScriptUri)
      .toJson.asJsObject.fields.mapValues(labelValue => labelValue.convertTo[String])

    // combine default and given labels and add labels for extensions
    val allLabels = clusterRequest.labels ++ defaultLabels ++
      clusterRequest.userJupyterExtensionConfig.map(_.asLabels).getOrElse(Map.empty)

    // check the labels do not contain forbidden keys
    if (allLabels.contains(includeDeletedKey))
      throw IllegalLabelKeyException(includeDeletedKey)
    else clusterRequest
      .copy(labels = allLabels)
  }

  private[service] def processClusterImages(clusterRequest: ClusterRequest): Set[ClusterImage] = {
    val now = Instant.now

    //If welder is enabled for this cluster, we need to ensure that an image is chosen.
    //We will use the client-supplied image, if present, otherwise we will use a default.
    //If welder is not enabled, we won't use any image.
    //Eventually welder will be enabled for all clusters and this will be way cleaner.
    val welderImageOpt: Option[ClusterImage] = if(clusterRequest.enableWelder.getOrElse(false)) {
      val i = clusterRequest.welderDockerImage.getOrElse(dataprocConfig.welderDockerImage)
      Some(ClusterImage(Welder, i, now))
    } else None

    // Note: Jupyter image is not currently optional
    val jupyterImage: ClusterImage = ClusterImage(Jupyter,
      clusterRequest.jupyterDockerImage.getOrElse(dataprocConfig.dataprocDockerImage), now)

    // Optional RStudio image
    val rstudioImageOpt: Option[ClusterImage] = clusterRequest.rstudioDockerImage.map(i => ClusterImage(RStudio, i, now))

    Set(welderImageOpt, Some(jupyterImage), rstudioImageOpt).flatten
  }

}
