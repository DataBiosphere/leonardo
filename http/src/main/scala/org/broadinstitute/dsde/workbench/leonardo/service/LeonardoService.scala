package org.broadinstitute.dsde.workbench.leonardo
package service

import java.text.SimpleDateFormat
import java.time.Instant
import java.util.UUID

import _root_.io.chrisdavenport.log4cats.Logger
import akka.actor.ActorSystem
import akka.http.scaladsl.model.StatusCodes
import cats.Monoid
import cats.data.{Ior, OptionT}
import cats.effect.{ContextShift, IO}
import cats.implicits._
import cats.mtl.ApplicativeAsk
import com.google.api.client.googleapis.json.GoogleJsonResponseException
import com.google.api.client.http.HttpResponseException
import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.dsde.workbench.google.GoogleStorageDAO
import org.broadinstitute.dsde.workbench.leonardo.config._
import org.broadinstitute.dsde.workbench.leonardo.dao.google._
import org.broadinstitute.dsde.workbench.leonardo.dao.{DockerDAO, WelderDAO}
import org.broadinstitute.dsde.workbench.leonardo.db.DbReference
import org.broadinstitute.dsde.workbench.leonardo.model.Cluster.LabelMap
import org.broadinstitute.dsde.workbench.leonardo.model.ClusterImageType.{Jupyter, Welder}
import org.broadinstitute.dsde.workbench.leonardo.model.LeonardoJsonSupport._
import org.broadinstitute.dsde.workbench.leonardo.model.NotebookClusterActions._
import org.broadinstitute.dsde.workbench.leonardo.model.ProjectActions._
import org.broadinstitute.dsde.workbench.leonardo.model.WelderAction._
import org.broadinstitute.dsde.workbench.leonardo.model._
import org.broadinstitute.dsde.workbench.leonardo.model.google.ClusterStatus.Stopped
import org.broadinstitute.dsde.workbench.leonardo.model.google._
import org.broadinstitute.dsde.workbench.leonardo.util.{BucketHelper, ClusterHelper}
import org.broadinstitute.dsde.workbench.model.google._
import org.broadinstitute.dsde.workbench.model.{TraceId, UserInfo, WorkbenchEmail}
import org.broadinstitute.dsde.workbench.newrelic.NewRelicMetrics
import org.broadinstitute.dsde.workbench.util.Retry
import spray.json._

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._
import scala.util.Try
import scala.util.control.NonFatal

case class AuthorizationError(email: Option[WorkbenchEmail] = None)
    extends LeoException(s"${email.map(e => s"'${e.value}'").getOrElse("Your account")} is unauthorized",
                         StatusCodes.Forbidden)

case class ClusterNotFoundException(googleProject: GoogleProject, clusterName: ClusterName)
    extends LeoException(s"Cluster ${googleProject.value}/${clusterName.value} not found", StatusCodes.NotFound)

case class ClusterAlreadyExistsException(googleProject: GoogleProject, clusterName: ClusterName, status: ClusterStatus)
    extends LeoException(
      s"Cluster ${googleProject.value}/${clusterName.value} already exists in ${status.toString} status",
      StatusCodes.Conflict
    )

case class ClusterCannotBeStoppedException(googleProject: GoogleProject,
                                           clusterName: ClusterName,
                                           status: ClusterStatus)
    extends LeoException(
      s"Cluster ${googleProject.value}/${clusterName.value} cannot be stopped in ${status.toString} status",
      StatusCodes.Conflict
    )

case class ClusterCannotBeDeletedException(googleProject: GoogleProject, clusterName: ClusterName)
    extends LeoException(s"Cluster ${googleProject.value}/${clusterName.value} cannot be deleted in Creating status",
                         StatusCodes.Conflict)

case class ClusterCannotBeStartedException(googleProject: GoogleProject,
                                           clusterName: ClusterName,
                                           status: ClusterStatus)
    extends LeoException(
      s"Cluster ${googleProject.value}/${clusterName.value} cannot be started in ${status.toString} status",
      StatusCodes.Conflict
    )

case class ClusterOutOfDateException()
    extends LeoException(
      "Your notebook runtime is out of date, and cannot be started due to recent updates in Terra. If you generated " +
        "data or copied external files to the runtime that you want to keep please contact support by emailing " +
        "Terra-support@broadinstitute.zendesk.com. Otherwise, simply delete your existing runtime and create a new one.",
      StatusCodes.Conflict
    )

case class ClusterCannotBeUpdatedException(cluster: Cluster)
    extends LeoException(s"Cluster ${cluster.projectNameString} cannot be updated in ${cluster.status} status",
                         StatusCodes.Conflict)

case class ClusterMachineTypeCannotBeChangedException(cluster: Cluster)
    extends LeoException(
      s"Cluster ${cluster.projectNameString} in ${cluster.status} status must be stopped in order to change machine type",
      StatusCodes.Conflict
    )

case class ClusterDiskSizeCannotBeDecreasedException(cluster: Cluster)
    extends LeoException(s"Cluster ${cluster.projectNameString}: decreasing master disk size is not allowed",
                         StatusCodes.PreconditionFailed)

case class BucketObjectException(gcsUri: String)
    extends LeoException(s"The provided GCS URI is invalid or unparseable: ${gcsUri}", StatusCodes.BadRequest)

case class BucketObjectAccessException(userEmail: WorkbenchEmail, gcsUri: GcsPath)
    extends LeoException(s"${userEmail.value} does not have access to ${gcsUri.toUri}", StatusCodes.Forbidden)

case class ParseLabelsException(labelString: String)
    extends LeoException(s"Could not parse label string: $labelString. Expected format [key1=value1,key2=value2,...]",
                         StatusCodes.BadRequest)

case class IllegalLabelKeyException(labelKey: String)
    extends LeoException(s"Labels cannot have a key of '$labelKey'", StatusCodes.NotAcceptable)

case class InvalidDataprocMachineConfigException(errorMsg: String)
    extends LeoException(s"${errorMsg}", StatusCodes.BadRequest)

case class ImageAutoDetectionException(traceId: TraceId, image: ContainerImage)
    extends LeoException(s"${traceId} | Unable to auto-detect tool for ${image.registry} image ${image.imageUrl}",
                         StatusCodes.Conflict)

class LeonardoService(protected val dataprocConfig: DataprocConfig,
                      protected val welderDao: WelderDAO[IO],
                      protected val clusterDefaultsConfig: ClusterDefaultsConfig,
                      protected val proxyConfig: ProxyConfig,
                      protected val swaggerConfig: SwaggerConfig,
                      protected val autoFreezeConfig: AutoFreezeConfig,
                      protected val petGoogleStorageDAO: String => GoogleStorageDAO,
                      protected val dbRef: DbReference,
                      protected val authProvider: LeoAuthProvider[IO],
                      protected val serviceAccountProvider: ServiceAccountProvider[IO],
                      protected val bucketHelper: BucketHelper,
                      protected val clusterHelper: ClusterHelper,
                      protected val dockerDAO: DockerDAO[IO])(implicit val executionContext: ExecutionContext,
                                                              implicit override val system: ActorSystem,
                                                              log: Logger[IO],
                                                              cs: ContextShift[IO],
                                                              metrics: NewRelicMetrics[IO])
    extends LazyLogging
    with Retry {

  private val bucketPathMaxLength = 1024
  private val includeDeletedKey = "includeDeleted"

  protected def checkProjectPermission(userInfo: UserInfo, action: ProjectAction, project: GoogleProject)(
    implicit ev: ApplicativeAsk[IO, TraceId]
  ): IO[Unit] =
    authProvider.hasProjectPermission(userInfo, action, project) flatMap {
      case false => IO.raiseError(AuthorizationError(Option(userInfo.userEmail)))
      case true  => IO.unit
    }

  // Throws 404 and pretends we don't even know there's a cluster there, by default.
  // If the cluster really exists and you're OK with the user knowing that, set throw403 = true.
  protected def checkClusterPermission(userInfo: UserInfo,
                                       action: NotebookClusterAction,
                                       cluster: Cluster,
                                       throw403: Boolean = false)(implicit ev: ApplicativeAsk[IO, TraceId]): IO[Unit] =
    for {
      traceId <- ev.ask
      hasPermission <- authProvider.hasNotebookClusterPermission(cluster.internalId,
                                                                 userInfo,
                                                                 action,
                                                                 cluster.googleProject,
                                                                 cluster.clusterName)
      _ <- hasPermission match {
        case false =>
          log
            .warn(
              s"${traceId} | User ${userInfo.userEmail} does not have the notebook permission ${action} for " +
                s"${cluster.googleProject}/${cluster.clusterName}"
            )
            .flatMap { _ =>
              if (throw403)
                IO.raiseError(AuthorizationError(Some(userInfo.userEmail)))
              else
                IO.raiseError(ClusterNotFoundException(cluster.googleProject, cluster.clusterName))
            }
        case true => IO.unit
      }
    } yield ()

  // We complete the API response without waiting for the cluster to be created
  // on the Google Dataproc side, which happens asynchronously to the request
  def createCluster(
    userInfo: UserInfo,
    googleProject: GoogleProject,
    clusterName: ClusterName,
    clusterRequest: ClusterRequest
  )(implicit ev: ApplicativeAsk[IO, TraceId]): IO[Cluster] =
    for {
      _ <- checkProjectPermission(userInfo, CreateClusters, googleProject)

      // Grab the service accounts from serviceAccountProvider for use later
      clusterServiceAccountOpt <- serviceAccountProvider
        .getClusterServiceAccount(userInfo, googleProject)
      notebookServiceAccountOpt <- serviceAccountProvider
        .getNotebookServiceAccount(userInfo, googleProject)
      serviceAccountInfo = ServiceAccountInfo(clusterServiceAccountOpt, notebookServiceAccountOpt)

      clusterOpt <- dbRef.inTransactionIO { _.clusterQuery.getActiveClusterByNameMinimal(googleProject, clusterName) }

      cluster <- clusterOpt.fold(
        internalCreateCluster(userInfo.userEmail, serviceAccountInfo, googleProject, clusterName, clusterRequest)
      )(c => IO.raiseError(ClusterAlreadyExistsException(googleProject, clusterName, c.status)))

    } yield cluster

  private def internalCreateCluster(
    userEmail: WorkbenchEmail,
    serviceAccountInfo: ServiceAccountInfo,
    googleProject: GoogleProject,
    clusterName: ClusterName,
    clusterRequest: ClusterRequest
  )(implicit ev: ApplicativeAsk[IO, TraceId]): IO[Cluster] =
    // Validate that the Jupyter extension URIs and Jupyter user script URI are valid URIs and reference real GCS objects
    // and if so, save the cluster creation request parameters in DB
    for {
      traceId <- ev.ask
      internalId <- IO(ClusterInternalId(UUID.randomUUID().toString))
      clusterImages <- getClusterImages(clusterRequest)
      augmentedClusterRequest = augmentClusterRequest(serviceAccountInfo,
                                                      googleProject,
                                                      clusterName,
                                                      userEmail,
                                                      clusterRequest,
                                                      clusterImages)
      machineConfig = MachineConfigOps.create(clusterRequest.machineConfig, clusterDefaultsConfig)
      autopauseThreshold = calculateAutopauseThreshold(clusterRequest.autopause, clusterRequest.autopauseThreshold)
      clusterScopes = if (clusterRequest.scopes.isEmpty) dataprocConfig.defaultScopes else clusterRequest.scopes
      initialClusterToSave = Cluster.create(
        augmentedClusterRequest,
        internalId,
        userEmail,
        clusterName,
        googleProject,
        serviceAccountInfo,
        machineConfig,
        dataprocConfig.clusterUrlBase,
        autopauseThreshold,
        clusterScopes,
        clusterImages
      )
      _ <- log.info(
        s"[$traceId] will deploy the following images to cluster ${initialClusterToSave.projectNameString}: ${clusterImages}"
      )
      _ <- validateClusterRequestBucketObjectUri(userEmail, googleProject, augmentedClusterRequest)
      _ <- log.info(
        s"[$traceId] Attempting to notify the AuthProvider for creation of cluster ${initialClusterToSave.projectNameString}"
      )
      _ <- authProvider.notifyClusterCreated(internalId, userEmail, googleProject, clusterName).handleErrorWith { t =>
        log.info(
          s"[$traceId] Failed to notify the AuthProvider for creation of cluster ${initialClusterToSave.projectNameString}"
        ) >> IO.raiseError(t)
      }
      cluster <- dbRef.inTransactionIO { _.clusterQuery.save(initialClusterToSave) }
      _ <- log.info(
        s"[$traceId] Inserted an initial record into the DB for cluster ${cluster.projectNameString}"
      )
    } yield cluster

  // throws 404 if nonexistent or no permissions
  def getActiveClusterDetails(userInfo: UserInfo, googleProject: GoogleProject, clusterName: ClusterName)(
    implicit ev: ApplicativeAsk[IO, TraceId]
  ): IO[Cluster] =
    for {
      cluster <- internalGetActiveClusterDetails(googleProject, clusterName) //throws 404 if nonexistent
      _ <- checkClusterPermission(userInfo, GetClusterStatus, cluster) //throws 404 if no auth
    } yield cluster

  private def internalGetActiveClusterDetails(googleProject: GoogleProject, clusterName: ClusterName): IO[Cluster] =
    dbRef.inTransactionIO { dataAccess =>
      dataAccess.clusterQuery.getActiveClusterByName(googleProject, clusterName)
    } flatMap {
      case None          => IO.raiseError(ClusterNotFoundException(googleProject, clusterName))
      case Some(cluster) => IO.pure(cluster)
    }

  def updateCluster(userInfo: UserInfo,
                    googleProject: GoogleProject,
                    clusterName: ClusterName,
                    clusterRequest: ClusterRequest)(implicit ev: ApplicativeAsk[IO, TraceId]): IO[Cluster] =
    for {
      cluster <- getActiveClusterDetails(userInfo, googleProject, clusterName) //throws 404 if nonexistent or no auth
      updatedCluster <- internalUpdateCluster(cluster, clusterRequest)
    } yield updatedCluster

  def internalUpdateCluster(existingCluster: Cluster, clusterRequest: ClusterRequest): IO[Cluster] = {
    implicit val booleanSumMonoidInstance = new Monoid[Boolean] {
      def empty = false
      def combine(a: Boolean, b: Boolean): Boolean = a || b
    }

    if (existingCluster.status.isUpdatable) {
      for {
        autopauseChanged <- maybeUpdateAutopauseThreshold(existingCluster,
                                                          clusterRequest.autopause,
                                                          clusterRequest.autopauseThreshold).attempt

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
        now <- IO(Instant.now)
        _ <- if (shouldUpdate.combineAll) {
          dbRef.inTransactionIO { _.clusterQuery.updateClusterStatus(existingCluster.id, ClusterStatus.Updating, now) }.void
        } else IO.unit

        cluster <- errors match {
          case Nil => internalGetActiveClusterDetails(existingCluster.googleProject, existingCluster.clusterName)
          // Just return the first error; we don't have a great mechanism to return all errors
          case h :: _ => IO.raiseError(h)
        }
      } yield cluster

    } else IO.raiseError(ClusterCannotBeUpdatedException(existingCluster))
  }

  private def getUpdatedValueIfChanged[A](existing: Option[A], updated: Option[A]): Option[A] =
    (existing, updated) match {
      case (None, Some(0)) =>
        None //An updated value of 0 is considered the same as None to prevent google APIs from complaining
      case (_, Some(x)) if updated != existing => Some(x)
      case _                                   => None
    }

  def maybeUpdateAutopauseThreshold(existingCluster: Cluster,
                                    autopause: Option[Boolean],
                                    autopauseThreshold: Option[Int]): IO[Boolean] = {
    val updatedAutopauseThresholdOpt = getUpdatedValueIfChanged(
      Option(existingCluster.autopauseThreshold),
      Option(calculateAutopauseThreshold(autopause, autopauseThreshold))
    )
    updatedAutopauseThresholdOpt match {
      case Some(updatedAutopauseThreshold) =>
        for {
          _ <- log.info(s"Changing autopause threshold for cluster ${existingCluster.projectNameString}")
          now <- IO(Instant.now)
          res <- dbRef
            .inTransactionIO { dataAccess =>
              dataAccess.clusterQuery.updateAutopauseThreshold(existingCluster.id, updatedAutopauseThreshold, now)
            }
            .as(true)
        } yield res

      case None => IO.pure(false)
    }
  }

  //returns true if cluster was resized, otherwise returns false
  def maybeResizeCluster(existingCluster: Cluster, machineConfigOpt: Option[MachineConfig]): IO[Boolean] = {
    //machineConfig.numberOfPreemtible undefined, and a 0 is passed in
    //
    val updatedNumWorkersAndPreemptiblesOpt = machineConfigOpt.flatMap { machineConfig =>
      Ior.fromOptions(
        getUpdatedValueIfChanged(existingCluster.machineConfig.numberOfWorkers, machineConfig.numberOfWorkers),
        getUpdatedValueIfChanged(existingCluster.machineConfig.numberOfPreemptibleWorkers,
                                 machineConfig.numberOfPreemptibleWorkers)
      )
    }

    updatedNumWorkersAndPreemptiblesOpt match {
      case Some(updatedNumWorkersAndPreemptibles) =>
        for {
          _ <- log.info(s"New machine config present. Resizing cluster ${existingCluster.projectNameString}...")
          // Resize the cluster
          _ <- clusterHelper.resizeCluster(existingCluster,
                                           updatedNumWorkersAndPreemptibles.left,
                                           updatedNumWorkersAndPreemptibles.right) recoverWith {
            case gjre: GoogleJsonResponseException =>
              // Typically we will revoke this role in the monitor after everything is complete, but if Google fails to
              // resize the cluster we need to revoke it manually here
              for {
                _ <- clusterHelper.removeClusterIamRoles(existingCluster.googleProject,
                                                         existingCluster.serviceAccountInfo)
                // Remove member from the Google Group that has the IAM role to pull the Dataproc image
                _ <- clusterHelper.updateDataprocImageGroupMembership(existingCluster.googleProject,
                                                                      createCluster = false)
                _ <- log.error(gjre)(s"Could not successfully update cluster ${existingCluster.projectNameString}")
                _ <- IO.raiseError[Unit](InvalidDataprocMachineConfigException(gjre.getMessage))
              } yield ()
          }

          // Update the DB
          now <- IO(Instant.now)
          _ <- dbRef.inTransactionIO { dataAccess =>
            updatedNumWorkersAndPreemptibles.fold(
              a => dataAccess.clusterQuery.updateNumberOfWorkers(existingCluster.id, a, now),
              a => dataAccess.clusterQuery.updateNumberOfPreemptibleWorkers(existingCluster.id, Option(a), now),
              (a, b) =>
                dataAccess.clusterQuery
                  .updateNumberOfWorkers(existingCluster.id, a, now)
                  .flatMap(
                    _ => dataAccess.clusterQuery.updateNumberOfPreemptibleWorkers(existingCluster.id, Option(b), now)
                  )
            )
          }
        } yield true

      case None => IO.pure(false)
    }
  }

  def maybeChangeMasterMachineType(existingCluster: Cluster, machineConfigOpt: Option[MachineConfig]): IO[Boolean] = {
    val updatedMasterMachineTypeOpt = machineConfigOpt.flatMap { machineConfig =>
      getUpdatedValueIfChanged(existingCluster.machineConfig.masterMachineType, machineConfig.masterMachineType)
    }

    updatedMasterMachineTypeOpt match {
      // Note: instance must be stopped in order to change machine type
      // TODO future enchancement: add capability to Leo to manage stop/update/restart transitions itself.
      case Some(updatedMasterMachineType) if existingCluster.status == Stopped =>
        for {
          _ <- log.info(
            s"New machine config present. Changing machine type to ${updatedMasterMachineType} for cluster ${existingCluster.projectNameString}..."
          )
          // Update the machine type in Google
          _ <- clusterHelper.setMasterMachineType(existingCluster, MachineType(updatedMasterMachineType))
          // Update the DB
          now <- IO(Instant.now)
          _ <- dbRef.inTransactionIO {
            _.clusterQuery.updateMasterMachineType(existingCluster.id, MachineType(updatedMasterMachineType), now)
          }
        } yield true

      case Some(_) =>
        IO.raiseError(ClusterMachineTypeCannotBeChangedException(existingCluster))

      case None =>
        IO.pure(false)
    }
  }

  def maybeChangeMasterDiskSize(existingCluster: Cluster, machineConfigOpt: Option[MachineConfig]): IO[Boolean] = {
    val updatedMasterDiskSizeOpt = machineConfigOpt.flatMap { machineConfig =>
      getUpdatedValueIfChanged(existingCluster.machineConfig.masterDiskSize, machineConfig.masterDiskSize)
    }

    // Note: GCE allows you to increase a persistent disk, but not decrease. Throw an exception if the user tries to decrease their disk.
    val diskSizeIncreased = (newSize: Int) => existingCluster.machineConfig.masterDiskSize.exists(_ < newSize)

    updatedMasterDiskSizeOpt match {
      case Some(updatedMasterDiskSize) if diskSizeIncreased(updatedMasterDiskSize) =>
        for {
          _ <- log.info(
            s"New machine config present. Changing master disk size to $updatedMasterDiskSize GB for cluster ${existingCluster.projectNameString}..."
          )
          // Update the disk in Google
          _ <- clusterHelper.updateMasterDiskSize(existingCluster, updatedMasterDiskSize)
          // Update the DB
          now <- IO(Instant.now)
          _ <- dbRef.inTransactionIO {
            _.clusterQuery.updateMasterDiskSize(existingCluster.id, updatedMasterDiskSize, now)
          }
        } yield true

      case Some(_) =>
        IO.raiseError(ClusterDiskSizeCannotBeDecreasedException(existingCluster))

      case None =>
        IO.pure(false)
    }
  }

  def deleteCluster(userInfo: UserInfo, googleProject: GoogleProject, clusterName: ClusterName)(
    implicit ev: ApplicativeAsk[IO, TraceId]
  ): IO[Unit] =
    for {
      //throws 404 if no permissions
      cluster <- getActiveClusterDetails(userInfo, googleProject, clusterName)

      //if you've got to here you at least have GetClusterDetails permissions so a 403 is appropriate if you can't actually destroy it
      _ <- checkClusterPermission(userInfo, DeleteCluster, cluster, throw403 = true)

      _ <- internalDeleteCluster(userInfo.userEmail, cluster)
    } yield ()

  //NOTE: This function MUST ALWAYS complete ALL steps. i.e. if deleting thing1 fails, it must still proceed to delete thing2
  def internalDeleteCluster(userEmail: WorkbenchEmail,
                            cluster: Cluster)(implicit ev: ApplicativeAsk[IO, TraceId]): IO[Unit] =
    if (cluster.status.isDeletable) {
      for {
        // Delete the notebook service account key in Google, if present
        keyIdOpt <- dbRef.inTransactionIO {
          _.clusterQuery.getServiceAccountKeyId(cluster.googleProject, cluster.clusterName)
        }
        _ <- clusterHelper
          .removeServiceAccountKey(cluster.googleProject, cluster.serviceAccountInfo.notebookServiceAccount, keyIdOpt)
          .recoverWith {
            case NonFatal(e) =>
              log.error(e)(
                s"Error occurred removing service account key for ${cluster.googleProject} / ${cluster.clusterName}"
              )
          }
        hasDataprocInfo = cluster.dataprocInfo.isDefined
        // Delete the cluster in Google
        _ <- if (hasDataprocInfo) clusterHelper.deleteCluster(cluster) else IO.unit
        // Change the cluster status to Deleting in the database
        // Note this also changes the instance status to Deleting
        now <- IO(Instant.now)
        _ <- dbRef.inTransactionIO { dataAccess =>
          if (hasDataprocInfo) dataAccess.clusterQuery.markPendingDeletion(cluster.id, now)
          else dataAccess.clusterQuery.completeDeletion(cluster.id, now)
        }
        _ <- if (hasDataprocInfo) IO.unit
        else
          authProvider
            .notifyClusterDeleted(cluster.internalId,
                                  cluster.auditInfo.creator,
                                  cluster.auditInfo.creator,
                                  cluster.googleProject,
                                  cluster.clusterName)
      } yield ()
    } else if (cluster.status == ClusterStatus.Creating) {
      IO.raiseError(ClusterCannotBeDeletedException(cluster.googleProject, cluster.clusterName))
    } else IO.unit

  def stopCluster(userInfo: UserInfo, googleProject: GoogleProject, clusterName: ClusterName)(
    implicit ev: ApplicativeAsk[IO, TraceId]
  ): IO[Unit] =
    for {
      //throws 404 if no permissions
      cluster <- getActiveClusterDetails(userInfo, googleProject, clusterName)

      //if you've got to here you at least have GetClusterDetails permissions so a 403 is appropriate if you can't actually stop it
      _ <- checkClusterPermission(userInfo, StopStartCluster, cluster, throw403 = true)

      _ <- internalStopCluster(cluster)
    } yield ()

  def internalStopCluster(cluster: Cluster): IO[Unit] =
    if (cluster.status.isStoppable) {
      for {
        // Flush the welder cache to disk
        _ <- if (cluster.welderEnabled) {
          welderDao
            .flushCache(cluster.googleProject, cluster.clusterName)
            .handleErrorWith(e => log.error(e)(s"Failed to flush welder cache for ${cluster}"))
        } else IO.unit

        // Stop the cluster in Google
        _ <- clusterHelper.stopCluster(cluster)

        // Update the cluster status to Stopping
        now <- IO(Instant.now)
        _ <- dbRef.inTransactionIO { _.clusterQuery.setToStopping(cluster.id, now) }
      } yield ()

    } else IO.raiseError(ClusterCannotBeStoppedException(cluster.googleProject, cluster.clusterName, cluster.status))

  def startCluster(userInfo: UserInfo, googleProject: GoogleProject, clusterName: ClusterName)(
    implicit ev: ApplicativeAsk[IO, TraceId]
  ): IO[Unit] =
    for {
      //throws 404 if no permissions
      cluster <- getActiveClusterDetails(userInfo, googleProject, clusterName)

      //if you've got to here you at least have GetClusterDetails permissions so a 403 is appropriate if you can't actually stop it
      _ <- checkClusterPermission(userInfo, StopStartCluster, cluster, throw403 = true)

      _ <- internalStartCluster(userInfo.userEmail, cluster)
    } yield ()

  def internalStartCluster(userEmail: WorkbenchEmail, cluster: Cluster): IO[Unit] =
    if (cluster.status.isStartable) {
      val welderAction = getWelderAction(cluster)
      for {
        // Check if welder should be deployed or updated
        updatedCluster <- welderAction match {
          case DeployWelder | UpdateWelder      => updateWelder(cluster)
          case NoAction | DisableDelocalization => IO.pure(cluster)
          case ClusterOutOfDate                 => IO.raiseError(ClusterOutOfDateException())
        }
        _ <- if (welderAction == DisableDelocalization && !cluster.labels.contains("welderInstallFailed"))
          dbRef.inTransactionIO { _.labelQuery.save(cluster.id, "welderInstallFailed", "true") } else IO.unit

        // Start the cluster in Google
        _ <- clusterHelper.startCluster(updatedCluster, welderAction)

        // Update the cluster status to Starting
        now <- IO(Instant.now)
        _ <- dbRef.inTransactionIO { dataAccess =>
          dataAccess.clusterQuery.updateClusterStatus(updatedCluster.id, ClusterStatus.Starting, now)
        }
      } yield ()
    } else IO.raiseError(ClusterCannotBeStartedException(cluster.googleProject, cluster.clusterName, cluster.status))

  def listClusters(userInfo: UserInfo, params: LabelMap, googleProjectOpt: Option[GoogleProject] = None)(
    implicit ev: ApplicativeAsk[IO, TraceId]
  ): IO[Vector[ListClusterResponse]] =
    for {
      paramMap <- processListClustersParameters(params)
      clusterList <- dbRef.inTransactionIO { da =>
        da.clusterQuery.listByLabels(paramMap._1, paramMap._2, googleProjectOpt)
      }
      samVisibleClusters <- authProvider
        .filterUserVisibleClusters(userInfo, clusterList.map(c => (c.googleProject, c.internalId)).toList)
    } yield {
      // Making the assumption that users will always be able to access clusters that they create
      // Fix for https://github.com/DataBiosphere/leonardo/issues/821
      val userCreatedClusters: List[(GoogleProject, ClusterInternalId)] = clusterList
        .filter(_.auditInfo.creator == userInfo.userEmail)
        .map(c => (c.googleProject, c.internalId))
        .toList

      val visibleClustersSet = (samVisibleClusters ::: userCreatedClusters).toSet
      clusterList.collect {
        case c if visibleClustersSet.contains((c.googleProject, c.internalId)) =>
          c.toListClusterResp
      }.toVector
    }

  private def calculateAutopauseThreshold(autopause: Option[Boolean], autopauseThreshold: Option[Int]): Int =
    autopause match {
      case None =>
        autoFreezeConfig.autoFreezeAfter.toMinutes.toInt
      case Some(false) =>
        autoPauseOffValue
      case _ =>
        if (autopauseThreshold.isEmpty) autoFreezeConfig.autoFreezeAfter.toMinutes.toInt
        else Math.max(autoPauseOffValue, autopauseThreshold.get)
    }

  private def validateClusterRequestBucketObjectUri(
    userEmail: WorkbenchEmail,
    googleProject: GoogleProject,
    clusterRequest: ClusterRequest
  )(implicit ev: ApplicativeAsk[IO, TraceId]): IO[Unit] = {
    val transformed = for {
      // Get a pet token from Sam. If we can't get a token, we won't do validation but won't fail cluster creation.
      petToken <- OptionT(serviceAccountProvider.getAccessToken(userEmail, googleProject).recoverWith {
        case e =>
          log.warn(e)(
            s"Could not acquire pet service account access token for user ${userEmail.value} in project $googleProject. " +
              s"Skipping validation of bucket objects in the cluster request."
          ) as None
      })

      // Validate the user script URIs
      _ <- clusterRequest.jupyterUserScriptUri match {
        case Some(userScriptUri) =>
          OptionT.liftF[IO, Unit](validateBucketObjectUri(userEmail, petToken, userScriptUri.toUri))
        case None => OptionT.pure[IO](())
      }
      _ <- clusterRequest.jupyterStartUserScriptUri match {
        case Some(startScriptUri) =>
          OptionT.liftF[IO, Unit](validateBucketObjectUri(userEmail, petToken, startScriptUri.toUri))
        case None => OptionT.pure[IO](())
      }

      // Validate the extension URIs
      _ <- clusterRequest.userJupyterExtensionConfig match {
        case Some(config) =>
          val extensionsToValidate =
            (config.nbExtensions.values ++ config.serverExtensions.values ++ config.combinedExtensions.values)
              .filter(_.startsWith("gs://"))
              .toList
          OptionT.liftF(extensionsToValidate.traverse(x => validateBucketObjectUri(userEmail, petToken, x)))
        case None => OptionT.pure[IO](())
      }
    } yield ()

    // Because of how OptionT works, `transformed.value` returns a Future[Option[Unit]]. `void` converts this to a Future[Unit].
    transformed.value.void
  }

  private[service] def validateBucketObjectUri(userEmail: WorkbenchEmail,
                                               userToken: String,
                                               gcsUri: String): IO[Unit] = {
    logger.debug(s"Validating user [${userEmail.value}] has access to bucket object $gcsUri")
    val gcsUriOpt = parseGcsPath(gcsUri)
    gcsUriOpt match {
      case Left(_)                                                      => IO.raiseError(BucketObjectException(gcsUri))
      case Right(gcsPath) if gcsPath.toUri.length > bucketPathMaxLength => IO.raiseError(BucketObjectException(gcsUri))
      case Right(gcsPath)                                               =>
        // Retry 401s from Google here because they can be thrown spuriously with valid credentials.
        // See https://github.com/DataBiosphere/leonardo/issues/460
        // Note GoogleStorageDAO already retries 500 and other errors internally, so we just need to catch 401s here.
        // We might think about moving the retry-on-401 logic inside GoogleStorageDAO.
        val errorMessage =
          s"GCS object validation failed for user [${userEmail.value}] and token [$userToken] and object [${gcsUri}]"
        IO.fromFuture[Boolean](
            IO(
              retryUntilSuccessOrTimeout(when401, errorMessage)(interval = 1 second, timeout = 3 seconds) { () =>
                petGoogleStorageDAO(userToken).objectExists(gcsPath.bucketName, gcsPath.objectName)
              }
            )
          )
          .flatMap {
            case true  => IO.unit
            case false => IO.raiseError(BucketObjectException(gcsPath.toUri))
          } recoverWith {
          case e: HttpResponseException if e.getStatusCode == StatusCodes.Forbidden.intValue =>
            log.error(e)(
              s"User ${userEmail.value} does not have access to ${gcsPath.bucketName} / ${gcsPath.objectName}"
            ) >> IO.raiseError(BucketObjectAccessException(userEmail, gcsPath))
          case e if when401(e) =>
            log.warn(e)(s"Could not validate object [${gcsUri}] as user [${userEmail.value}]")
        }
    }
  }

  private[service] def processListClustersParameters(params: LabelMap): IO[(LabelMap, Boolean)] =
    IO {
      params.get(includeDeletedKey) match {
        case Some(includeDeletedValue) => (processLabelMap(params - includeDeletedKey), includeDeletedValue.toBoolean)
        case None                      => (processLabelMap(params), false)
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
  private[service] def processLabelMap(params: LabelMap): LabelMap =
    params.get("_labels") match {
      case Some(extraLabels) =>
        extraLabels.split(',').foldLeft(Map.empty[String, String]) { (r, c) =>
          c.split('=') match {
            case Array(key, value) => r + (key -> value)
            case _                 => throw ParseLabelsException(extraLabels)
          }
        }
      case None => params
    }

  private[service] def augmentClusterRequest(serviceAccountInfo: ServiceAccountInfo,
                                             googleProject: GoogleProject,
                                             clusterName: ClusterName,
                                             userEmail: WorkbenchEmail,
                                             clusterRequest: ClusterRequest,
                                             clusterImages: Set[ClusterImage]): ClusterRequest = {
    val userJupyterExt = clusterRequest.jupyterExtensionUri match {
      case Some(ext) => Map[String, String]("notebookExtension" -> ext.toUri)
      case None      => Map[String, String]()
    }

    // add the userJupyterExt to the nbExtensions
    val updatedUserJupyterExtensionConfig = clusterRequest.userJupyterExtensionConfig match {
      case Some(config) => config.copy(nbExtensions = config.nbExtensions ++ userJupyterExt)
      case None         => UserJupyterExtensionConfig(userJupyterExt, Map.empty, Map.empty, Map.empty)
    }

    // transform Some(empty, empty, empty, empty) to None
    // TODO: is this really necessary?
    val updatedClusterRequest = clusterRequest.copy(
      userJupyterExtensionConfig =
        if (updatedUserJupyterExtensionConfig.asLabels.isEmpty)
          None
        else
          Some(updatedUserJupyterExtensionConfig)
    )

    addClusterLabels(serviceAccountInfo, googleProject, clusterName, userEmail, updatedClusterRequest, clusterImages)
  }

  private[service] def addClusterLabels(serviceAccountInfo: ServiceAccountInfo,
                                        googleProject: GoogleProject,
                                        clusterName: ClusterName,
                                        creator: WorkbenchEmail,
                                        clusterRequest: ClusterRequest,
                                        clusterImages: Set[ClusterImage]): ClusterRequest = {
    // create a LabelMap of default labels
    val defaultLabels = DefaultLabels(
      clusterName,
      googleProject,
      creator,
      serviceAccountInfo.clusterServiceAccount,
      serviceAccountInfo.notebookServiceAccount,
      clusterRequest.jupyterUserScriptUri,
      clusterRequest.jupyterStartUserScriptUri,
      clusterImages.map(_.imageType).filterNot(_ == Welder).headOption
    ).toJson.asJsObject.fields.mapValues(labelValue => labelValue.convertTo[String])

    // combine default and given labels and add labels for extensions
    val allLabels = clusterRequest.labels ++ defaultLabels ++
      clusterRequest.userJupyterExtensionConfig.map(_.asLabels).getOrElse(Map.empty)

    // check the labels do not contain forbidden keys
    if (allLabels.contains(includeDeletedKey))
      throw IllegalLabelKeyException(includeDeletedKey)
    else
      clusterRequest
        .copy(labels = allLabels)
  }

  private[service] def getClusterImages(
    clusterRequest: ClusterRequest
  )(implicit ev: ApplicativeAsk[IO, TraceId]): IO[Set[ClusterImage]] =
    for {
      now <- IO(Instant.now)
      traceId <- ev.ask
      // Try to autodetect the image
      autodetectedImageOpt <- clusterRequest.toolDockerImage.traverse { image =>
        dockerDAO.detectTool(image).flatMap {
          case None       => IO.raiseError(ImageAutoDetectionException(traceId, image))
          case Some(tool) => IO.pure(ClusterImage(tool, image.imageUrl, now))
        }
      }
      // Figure out the tool image. Rules:
      // - if we were able to autodetect an image, use that
      // - else if a legacy jupyterDockerImage param was sent, use that
      // - else use the default jupyter image
      jupyterImageOpt = clusterRequest.jupyterDockerImage.map(i => ClusterImage(Jupyter, i.imageUrl, now))
      defaultJupyterImage = ClusterImage(Jupyter, dataprocConfig.jupyterImage, now)
      toolImage = autodetectedImageOpt orElse jupyterImageOpt getOrElse defaultJupyterImage
      // Figure out the welder image. Rules:
      // - If welder is enabled, we will use the client-supplied image if present, otherwise we will use a default.
      // - If welder is not enabled, we won't use any image.
      welderImageOpt = if (clusterRequest.enableWelder.getOrElse(false)) {
        val imageUrl = clusterRequest.welderDockerImage.map(_.imageUrl).getOrElse(dataprocConfig.welderDockerImage)
        Some(ClusterImage(Welder, imageUrl, now))
      } else None
    } yield Set(Some(toolImage), welderImageOpt).flatten

  private def getWelderAction(cluster: Cluster): WelderAction =
    if (cluster.welderEnabled) {
      // Welder is already enabled; do we need to update it?
      val labelFound = dataprocConfig.updateWelderLabel.exists(cluster.labels.contains)

      val imageChanged = cluster.clusterImages.find(_.imageType == Welder) match {
        case Some(welderImage) if welderImage.imageUrl != dataprocConfig.welderDockerImage => true
        case _                                                                             => false
      }

      if (labelFound && imageChanged) UpdateWelder
      else NoAction
    } else {
      // Welder is not enabled; do we need to deploy it?
      val labelFound = dataprocConfig.deployWelderLabel.exists(cluster.labels.contains)
      if (labelFound) {
        if (isClusterBeforeCutoffDate(cluster)) DisableDelocalization
        else DeployWelder
      } else NoAction
    }

  private def isClusterBeforeCutoffDate(cluster: Cluster): Boolean =
    (for {
      dateStr <- dataprocConfig.deployWelderCutoffDate
      date <- Try(new SimpleDateFormat("yyyy-MM-dd").parse(dateStr)).toOption
      isClusterBeforeCutoffDate = cluster.auditInfo.createdDate.isBefore(date.toInstant)
    } yield isClusterBeforeCutoffDate) getOrElse false

  private def updateWelder(cluster: Cluster): IO[Cluster] =
    for {
      _ <- IO(logger.info(s"Will deploy welder to cluster ${cluster.projectNameString}"))
      _ <- metrics.incrementCounter("welder/deploy")
      now <- IO(Instant.now)
      welderImage = ClusterImage(Welder, dataprocConfig.welderDockerImage, now)
      _ <- dbRef.inTransactionIO {
        _.clusterQuery.updateWelder(cluster.id, ClusterImage(Welder, dataprocConfig.welderDockerImage, now), now)
      }
      newCluster = cluster.copy(welderEnabled = true,
                                clusterImages = cluster.clusterImages.filterNot(_.imageType == Welder) + welderImage)
    } yield newCluster

}
