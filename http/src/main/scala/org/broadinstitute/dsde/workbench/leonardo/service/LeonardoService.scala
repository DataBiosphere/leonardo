package org.broadinstitute.dsde.workbench.leonardo
package http
package service

import java.time.Instant
import java.util.UUID
import java.util.concurrent.TimeUnit

import _root_.io.chrisdavenport.log4cats.Logger
import akka.actor.ActorSystem
import akka.http.scaladsl.model.StatusCodes
import cats.Monoid
import cats.data.{Ior, OptionT}
import cats.effect.{ContextShift, IO, Timer}
import cats.implicits._
import cats.mtl.ApplicativeAsk
import com.google.api.client.googleapis.json.GoogleJsonResponseException
import com.google.api.client.http.HttpResponseException
import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.dsde.workbench.google.GoogleStorageDAO
import org.broadinstitute.dsde.workbench.google2.MachineTypeName
import org.broadinstitute.dsde.workbench.leonardo.RuntimeImageType.{Jupyter, Welder}
import org.broadinstitute.dsde.workbench.leonardo.RuntimeStatus.Stopped
import org.broadinstitute.dsde.workbench.leonardo.config._
import org.broadinstitute.dsde.workbench.leonardo.dao.google._
import org.broadinstitute.dsde.workbench.leonardo.dao.{DockerDAO, WelderDAO}
import org.broadinstitute.dsde.workbench.leonardo.db.{
  clusterQuery,
  DbReference,
  LeonardoServiceDbQueries,
  RuntimeConfigId,
  RuntimeConfigQueries,
  SaveCluster
}
import org.broadinstitute.dsde.workbench.leonardo.http.service.LeonardoService._
import org.broadinstitute.dsde.workbench.leonardo.http.service.UpdateTransition._
import org.broadinstitute.dsde.workbench.leonardo.model.NotebookClusterActions._
import org.broadinstitute.dsde.workbench.leonardo.model.ProjectActions._
import org.broadinstitute.dsde.workbench.leonardo.model._
import org.broadinstitute.dsde.workbench.leonardo.monitor.LeoPubsubMessage
import org.broadinstitute.dsde.workbench.leonardo.monitor.LeoPubsubMessage.StopUpdate
import org.broadinstitute.dsde.workbench.leonardo.util.{BucketHelper, ClusterHelper}
import org.broadinstitute.dsde.workbench.model.google._
import org.broadinstitute.dsde.workbench.model.{TraceId, UserInfo, WorkbenchEmail, WorkbenchException}
import org.broadinstitute.dsde.workbench.newrelic.NewRelicMetrics
import org.broadinstitute.dsde.workbench.util.Retry

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._
import scala.util.control.NonFatal

case class AuthorizationError(email: Option[WorkbenchEmail] = None)
    extends LeoException(s"${email.map(e => s"'${e.value}'").getOrElse("Your account")} is unauthorized",
                         StatusCodes.Forbidden)

case class RuntimeNotFoundException(googleProject: GoogleProject, runtimeName: RuntimeName)
    extends LeoException(s"Runtime ${googleProject.value}/${runtimeName.asString} not found", StatusCodes.NotFound)

case class RuntimeAlreadyExistsException(googleProject: GoogleProject, runtimeName: RuntimeName, status: RuntimeStatus)
    extends LeoException(
      s"Runtime ${googleProject.value}/${runtimeName.asString} already exists in ${status.toString} status",
      StatusCodes.Conflict
    )

case class RuntimeCannotBeStoppedException(googleProject: GoogleProject,
                                           runtimeName: RuntimeName,
                                           status: RuntimeStatus)
    extends LeoException(
      s"Runtime ${googleProject.value}/${runtimeName.asString} cannot be stopped in ${status.toString} status",
      StatusCodes.Conflict
    )

case class RuntimeCannotBeDeletedException(googleProject: GoogleProject, runtimeName: RuntimeName)
    extends LeoException(s"Runtime ${googleProject.value}/${runtimeName.asString} cannot be deleted in Creating status",
                         StatusCodes.Conflict)

case class RuntimeCannotBeStartedException(googleProject: GoogleProject,
                                           runtimeName: RuntimeName,
                                           status: RuntimeStatus)
    extends LeoException(
      s"Runtime ${googleProject.value}/${runtimeName.asString} cannot be started in ${status.toString} status",
      StatusCodes.Conflict
    )

case class RuntimeOutOfDateException()
    extends LeoException(
      "Your notebook runtime is out of date, and cannot be started due to recent updates in Terra. If you generated " +
        "data or copied external files to the runtime that you want to keep please contact support by emailing " +
        "Terra-support@broadinstitute.zendesk.com. Otherwise, simply delete your existing runtime and create a new one.",
      StatusCodes.Conflict
    )

case class RuntimeCannotBeUpdatedException(projectNameString: String, status: RuntimeStatus, userHint: String = "")
    extends LeoException(s"Runtime ${projectNameString} cannot be updated in ${status} status. ${userHint}",
                         StatusCodes.Conflict)

case class RuntimeMachineTypeCannotBeChangedException(runtime: Runtime)
    extends LeoException(
      s"Runtime ${runtime.projectNameString} in ${runtime.status} status must be stopped in order to change machine type. Some updates require stopping the runtime, or a re-create. If you wish Leonardo to handle this for you, investigate the allowStop and allowDelete flags for this API.",
      StatusCodes.Conflict
    )

case class RuntimeDiskSizeCannotBeDecreasedException(runtime: Runtime)
    extends LeoException(s"Runtime ${runtime.projectNameString}: decreasing master disk size is not allowed",
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

case class ImageNotFoundException(traceId: TraceId, image: ContainerImage)
    extends LeoException(s"${traceId} | Image ${image.imageUrl} not found", StatusCodes.NotFound)

case class UpdateResult(hasUpdateSucceded: Boolean, followupAction: Option[UpdateTransition])

sealed trait UpdateTransition extends Product with Serializable

object UpdateTransition {
  case class StopStartTransition(updateConfig: RuntimeConfig.DataprocConfig) extends UpdateTransition
  case class DeleteCreateTransition(updateConfig: RuntimeConfig.DataprocConfig) extends UpdateTransition
}

class LeonardoService(
  protected val dataprocConfig: DataprocConfig,
  protected val imageConfig: ImageConfig,
  protected val welderDao: WelderDAO[IO],
  protected val clusterDefaultsConfig: ClusterDefaultsConfig,
  protected val proxyConfig: ProxyConfig,
  protected val swaggerConfig: SwaggerConfig,
  protected val autoFreezeConfig: AutoFreezeConfig,
  protected val welderConfig: WelderConfig,
  protected val petGoogleStorageDAO: String => GoogleStorageDAO,
  protected val authProvider: LeoAuthProvider[IO],
  protected val serviceAccountProvider: ServiceAccountProvider[IO],
  protected val bucketHelper: BucketHelper,
  protected val clusterHelper: ClusterHelper,
  protected val dockerDAO: DockerDAO[IO],
  protected val publisherQueue: fs2.concurrent.Queue[IO, LeoPubsubMessage]
)(implicit val executionContext: ExecutionContext,
  implicit override val system: ActorSystem,
  log: Logger[IO],
  cs: ContextShift[IO],
  metrics: NewRelicMetrics[IO],
  dbRef: DbReference[IO],
  timer: Timer[IO])
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
                                       clusterInternalId: ClusterInternalId,
                                       runtimeProjectAndName: RuntimeProjectAndName,
                                       throw403: Boolean = false)(implicit ev: ApplicativeAsk[IO, TraceId]): IO[Unit] =
    for {
      traceId <- ev.ask
      hasPermission <- authProvider.hasNotebookClusterPermission(clusterInternalId,
                                                                 userInfo,
                                                                 action,
                                                                 runtimeProjectAndName.googleProject,
                                                                 runtimeProjectAndName.runtimeName)
      _ <- hasPermission match {
        case false =>
          log
            .warn(
              s"${traceId} | User ${userInfo.userEmail} does not have the notebook permission ${action} for " +
                s"${runtimeProjectAndName.googleProject}/${runtimeProjectAndName.runtimeName}"
            )
            .flatMap { _ =>
              if (throw403)
                IO.raiseError(AuthorizationError(Some(userInfo.userEmail)))
              else
                IO.raiseError(
                  RuntimeNotFoundException(runtimeProjectAndName.googleProject, runtimeProjectAndName.runtimeName)
                )
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
    clusterRequest: CreateRuntimeRequest
  )(implicit ev: ApplicativeAsk[IO, TraceId]): IO[CreateRuntimeAPIResponse] =
    for {
      _ <- checkProjectPermission(userInfo, CreateClusters, googleProject)
      // Grab the service accounts from serviceAccountProvider for use later
      clusterServiceAccountOpt <- serviceAccountProvider
        .getClusterServiceAccount(userInfo, googleProject)
      notebookServiceAccountOpt <- serviceAccountProvider
        .getNotebookServiceAccount(userInfo, googleProject)
      serviceAccountInfo = ServiceAccountInfo(clusterServiceAccountOpt, notebookServiceAccountOpt)

      clusterOpt <- clusterQuery.getActiveClusterByNameMinimal(googleProject, clusterName).transaction

      cluster <- clusterOpt.fold(
        internalCreateCluster(userInfo.userEmail, serviceAccountInfo, googleProject, clusterName, clusterRequest)
      )(c => IO.raiseError(RuntimeAlreadyExistsException(googleProject, clusterName, c.status)))

    } yield cluster

  private def internalCreateCluster(
    userEmail: WorkbenchEmail,
    serviceAccountInfo: ServiceAccountInfo,
    googleProject: GoogleProject,
    clusterName: ClusterName,
    clusterRequest: CreateRuntimeRequest
  )(implicit ev: ApplicativeAsk[IO, TraceId]): IO[CreateRuntimeAPIResponse] =
    // Validate that the Jupyter extension URIs and Jupyter user script URI are valid URIs and reference real GCS objects
    // and if so, save the cluster creation request parameters in DB
    for {
      traceId <- ev.ask
      internalId <- IO(RuntimeInternalId(UUID.randomUUID().toString))
      clusterImages <- getRuntimeImages(userEmail, googleProject, clusterRequest)
      augmentedClusterRequest = augmentClusterRequest(serviceAccountInfo,
                                                      googleProject,
                                                      clusterName,
                                                      userEmail,
                                                      clusterRequest,
                                                      clusterImages)
      // TODO
      defaultRuntimeConfig = null // MachineConfigOps.createFromDefaults(clusterDefaultsConfig)
      machineConfig = clusterRequest.runtimeConfig
        .asInstanceOf[Option[RuntimeConfigRequest.DataprocConfig]]
        .map(_.toRuntimeConfigDataprocConfig(defaultRuntimeConfig))
        .getOrElse(defaultRuntimeConfig) //TODO: remove this asInstanceOf
      now <- IO(Instant.now())
      autopauseThreshold = calculateAutopauseThreshold(clusterRequest.autopause, clusterRequest.autopauseThreshold)
      clusterScopes = if (clusterRequest.scopes.isEmpty) dataprocConfig.defaultScopes else clusterRequest.scopes
      // TODO
      initialClusterToSave: Runtime = null.asInstanceOf[Runtime]
//      Runtime.create(
//        augmentedClusterRequest,
//        internalId,
//        userEmail,
//        clusterName,
//        googleProject,
//        serviceAccountInfo,
//        machineConfig,
//        dataprocConfig.clusterUrlBase,
//        autopauseThreshold,
//        clusterScopes,
//        clusterImages
//      )
      _ <- log.info(
        s"[$traceId] will deploy the following images to cluster ${initialClusterToSave.projectNameString}: ${clusterImages}"
      )
      _ <- validateClusterRequestBucketObjectUri(userEmail, googleProject, augmentedClusterRequest) >> log.info(
        s"[$traceId] Attempting to notify the AuthProvider for creation of cluster ${initialClusterToSave.projectNameString}"
      )
      _ <- authProvider.notifyClusterCreated(internalId, userEmail, googleProject, clusterName).handleErrorWith { t =>
        log.info(
          s"[$traceId] Failed to notify the AuthProvider for creation of cluster ${initialClusterToSave.projectNameString}"
        ) >> IO.raiseError(t)
      }
      saveCluster = SaveCluster(cluster = initialClusterToSave, runtimeConfig = machineConfig, now = now)
      cluster <- clusterQuery.save(saveCluster).transaction
      _ <- log.info(
        s"[$traceId] Inserted an initial record into the DB for cluster ${cluster.projectNameString}"
      )
      // TODO
      _ <- publisherQueue.enqueue1(null) // (cluster.toCreateCluster(machineConfig, Some(traceId)))
    } yield CreateRuntimeAPIResponse.fromRuntime(cluster, machineConfig)

  // throws 404 if nonexistent or no permissions
  def getActiveClusterDetails(userInfo: UserInfo, googleProject: GoogleProject, clusterName: ClusterName)(
    implicit ev: ApplicativeAsk[IO, TraceId]
  ): IO[Cluster] =
    for {
      cluster <- internalGetActiveClusterDetails(googleProject, clusterName) //throws 404 if nonexistent
      _ <- checkClusterPermission(
        userInfo,
        GetClusterStatus,
        cluster.internalId,
        RuntimeProjectAndName(cluster.googleProject, cluster.runtimeName)
      ) //throws 404 if no auth
    } yield cluster

  // throws 404 if nonexistent or no permissions
  def getClusterAPI(userInfo: UserInfo, googleProject: GoogleProject, clusterName: ClusterName)(
    implicit ev: ApplicativeAsk[IO, TraceId]
  ): IO[GetRuntimeResponse] =
    for {
      resp <- LeonardoServiceDbQueries
        .getGetClusterResponse(googleProject, clusterName)
        .transaction //throws 404 if nonexistent
      _ <- checkClusterPermission(userInfo,
                                  GetClusterStatus,
                                  resp.internalId,
                                  RuntimeProjectAndName(resp.googleProject, resp.clusterName)) //throws 404 if no auth
    } yield resp

  private def internalGetActiveClusterDetails(googleProject: GoogleProject, clusterName: ClusterName): IO[Cluster] =
    clusterQuery.getActiveClusterByName(googleProject, clusterName).transaction.flatMap {
      case None          => IO.raiseError(RuntimeNotFoundException(googleProject, clusterName))
      case Some(cluster) => IO.pure(cluster)
    }

  def updateCluster(
    userInfo: UserInfo,
    googleProject: GoogleProject,
    clusterName: ClusterName,
    clusterRequest: CreateRuntimeRequest
  )(implicit ev: ApplicativeAsk[IO, TraceId]): IO[UpdateRuntimeResponse] =
    for {
      cluster <- getActiveClusterDetails(userInfo, googleProject, clusterName) //throws 404 if nonexistent or no auth
      runtimeConfig <- RuntimeConfigQueries.getRuntimeConfig(RuntimeConfigId(cluster.runtimeConfigId)).transaction
      updatedCluster <- internalUpdateCluster(cluster, clusterRequest, runtimeConfig)
    } yield UpdateRuntimeResponse.fromCluster(updatedCluster, runtimeConfig)

  def internalUpdateCluster(
    existingCluster: Cluster,
    clusterRequest: CreateRuntimeRequest,
    existingRuntimeConfig: RuntimeConfig
  )(implicit ev: ApplicativeAsk[IO, TraceId]): IO[Cluster] =
    if (existingCluster.status.isUpdatable) {
      for {
        autopauseChanged <- maybeUpdateAutopauseThreshold(existingCluster,
                                                          clusterRequest.autopause,
                                                          clusterRequest.autopauseThreshold).attempt

        (error, shouldUpdate, followUpAction) <- (existingRuntimeConfig, clusterRequest.runtimeConfig) match {
          case (existing, Some(target)) =>
            existing match {
              case _: RuntimeConfig.GceConfig => IO.raiseError(new NotImplementedError("GCE is not supported yet"))
              case existingRuntimeConfig: RuntimeConfig.DataprocConfig =>
                target match {
                  case _: RuntimeConfigRequest.GceConfig =>
                    IO.raiseError(
                      new WorkbenchException(
                        "Bad request. This runtime is created with GCE, and can not be updated to use dataproc"
                      )
                    ) //TODO: use better exception
                  case x: RuntimeConfigRequest.DataprocConfig =>
                    for {
                      clusterResized <- maybeResizeCluster(existingCluster,
                                                           existingRuntimeConfig,
                                                           x.numberOfWorkers,
                                                           x.numberOfPreemptibleWorkers).attempt
                      masterMachineTypeChanged <- maybeChangeMasterMachineType(existingCluster,
                                                                               existingRuntimeConfig,
                                                                               x.masterMachineType.map(MachineTypeName),
                                                                               clusterRequest.allowStop).attempt
                      masterDiskSizeChanged <- maybeChangeMasterDiskSize(existingCluster,
                                                                         existingRuntimeConfig,
                                                                         x.masterDiskSize).attempt
                    } yield {
                      val res = List(
                        autopauseChanged.map(_ => false),
                        clusterResized.map(_.hasUpdateSucceded),
                        masterMachineTypeChanged.map(_ => false),
                        masterDiskSizeChanged.map(_ => false)
                      ).separate
                      // Just return the first error; we don't have a great mechanism to return all errors
                      (
                        res._1.headOption,
                        res._2.exists(identity),
                        masterMachineTypeChanged.toOption.flatMap(_.followupAction)
                      )
                    }
                }
            }
          case _ =>
            IO.pure((None, false, None))
        }

        // Set the cluster status to Updating if the cluster was resized
        now <- IO(Instant.now)
        _ <- if (shouldUpdate) {
          clusterQuery.updateClusterStatus(existingCluster.id, RuntimeStatus.Updating, now).transaction.void
        } else IO.unit

        //maybeMasterMachineTypeChanged will throw an error if the appropriate flag hasn't been set to allow special update transitions
        _ <- followUpAction match {
          case Some(action) =>
            //we do not support follow-up transitions when the cluster is set to an updating status
            if (!shouldUpdate) {
              logger.info(
                s"detected follow-up action necessary for update on cluster ${existingCluster.projectNameString}: ${action}"
              )
              handleClusterTransition(existingCluster, action, now)
            } else
              IO.raiseError(
                RuntimeCannotBeUpdatedException(
                  existingCluster.projectNameString,
                  existingCluster.status,
                  "You cannot update the CPUs/Memory and the number of workers at the same time. We recommend you do this one at a time. The number of workers will be updated."
                )
              )
          case None =>
            IO(
              logger.debug(
                s"detected no follow-up action necessary for update on runtime ${existingCluster.projectNameString}"
              )
            )
        }

        cluster <- error match {
          case None    => internalGetActiveClusterDetails(existingCluster.googleProject, existingCluster.runtimeName)
          case Some(e) => IO.raiseError(e)
        }
      } yield cluster

    } else IO.raiseError(RuntimeCannotBeUpdatedException(existingCluster.projectNameString, existingCluster.status))

  private def handleClusterTransition(existingCluster: Cluster, transition: UpdateTransition, now: Instant)(
    implicit ev: ApplicativeAsk[IO, TraceId]
  ): IO[Unit] =
    transition match {
      case StopStartTransition(machineConfig) =>
        for {
          traceId <- ev.ask
          _ <- metrics.incrementCounter(s"pubsub/LeonardoService/StopStartTransition")
          //sends a message with the config to google pub/sub queue for processing by back leo
          _ <- publisherQueue.enqueue1(StopUpdate(machineConfig, existingCluster.id, Some(traceId)))
        } yield ()

      //TODO: we currently do not support this
      case DeleteCreateTransition(_) => IO.raiseError(new NotImplementedError())
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
                                    autopauseThreshold: Option[Int]): IO[UpdateResult] = {
    val updatedAutopauseThresholdOpt = getUpdatedValueIfChanged(
      Option(existingCluster.autopauseThreshold),
      Option(calculateAutopauseThreshold(autopause, autopauseThreshold))
    )
    updatedAutopauseThresholdOpt match {
      case Some(updatedAutopauseThreshold) =>
        for {
          _ <- log.info(s"Changing autopause threshold for cluster ${existingCluster.projectNameString}")
          now <- IO(Instant.now)
          res <- clusterQuery
            .updateAutopauseThreshold(existingCluster.id, updatedAutopauseThreshold, now)
            .transaction
            .as(UpdateResult(true, None))
        } yield res

      case None => IO.pure(UpdateResult(false, None))
    }
  }

  //returns true if cluster was resized, otherwise returns false
  def maybeResizeCluster(existingCluster: Cluster,
                         existingRuntimeConfig: RuntimeConfig.DataprocConfig,
                         targetNumberOfWorkers: Option[Int],
                         targetNumberOfPreemptibleWorkers: Option[Int]): IO[UpdateResult] = {
    //machineConfig.numberOfPreemtible undefined, and a 0 is passed in
    val updatedNumWorkersAndPreemptiblesOpt =
      Ior.fromOptions(
        getUpdatedValueIfChanged(Some(existingRuntimeConfig.numberOfWorkers), targetNumberOfWorkers),
        getUpdatedValueIfChanged(existingRuntimeConfig.numberOfPreemptibleWorkers, targetNumberOfPreemptibleWorkers)
      )

    updatedNumWorkersAndPreemptiblesOpt match {
      case Some(updatedNumWorkersAndPreemptibles) =>
        if (existingCluster.status != RuntimeStatus.Stopped) { //updating the number of workers in a stopped cluster can take forever, so we forbid it
          for {
            _ <- log.info(
              s"New numberOfWorkers($targetNumberOfWorkers) or numberOfPreemptibleWorkers($targetNumberOfPreemptibleWorkers) present. Resizing cluster ${existingCluster.projectNameString}..."
            )
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
            _ <- dbRef.inTransaction {
              updatedNumWorkersAndPreemptibles.fold(
                a =>
                  RuntimeConfigQueries.updateNumberOfWorkers(RuntimeConfigId(existingCluster.runtimeConfigId), a, now),
                a =>
                  RuntimeConfigQueries
                    .updateNumberOfPreemptibleWorkers(RuntimeConfigId(existingCluster.runtimeConfigId), Option(a), now),
                (a, b) =>
                  RuntimeConfigQueries
                    .updateNumberOfWorkers(RuntimeConfigId(existingCluster.runtimeConfigId), a, now)
                    .flatMap(
                      _ =>
                        RuntimeConfigQueries.updateNumberOfPreemptibleWorkers(
                          RuntimeConfigId(existingCluster.runtimeConfigId),
                          Option(b),
                          now
                        )
                    )
              )
            }
          } yield UpdateResult(true, None)
        } else
          IO.raiseError(
            RuntimeCannotBeUpdatedException(
              existingCluster.projectNameString,
              existingCluster.status,
              "You cannot update the number of workers in a stopped cluster. Please start your cluster to perform this action"
            )
          )

      case None => IO.pure(UpdateResult(false, None))
    }
  }

  def maybeChangeMasterMachineType(existingCluster: Cluster,
                                   existingRuntimeConfig: RuntimeConfig,
                                   targetMachineType: Option[MachineTypeName],
                                   allowStop: Boolean)(implicit ev: ApplicativeAsk[IO, TraceId]): IO[UpdateResult] = {
    val updatedMasterMachineTypeOpt =
      getUpdatedValueIfChanged(Some(existingRuntimeConfig.machineType), targetMachineType)

    updatedMasterMachineTypeOpt match {
      // Note: instance must be stopped in order to change machine type
      case Some(updatedMasterMachineType) if existingCluster.status == Stopped =>
        for {
          _ <- clusterHelper.updateMasterMachineType(existingCluster, updatedMasterMachineType)
        } yield UpdateResult(true, None)

      case Some(updatedMasterMachineType) =>
        existingRuntimeConfig match {
          case x: RuntimeConfig.DataprocConfig =>
            logger.debug("in stop and update case of maybeChangeMasterMachineType")
            val updatedConfig = x.copy(masterMachineType = updatedMasterMachineType.value)

            if (allowStop) {
              val transition = StopStartTransition(updatedConfig)
              logger.debug(
                s"detected stop and update transition specified in request of maybeChangeMasterMachineType, ${transition}"
              )
              IO.pure(UpdateResult(false, Some(transition)))
            } else IO.raiseError(RuntimeMachineTypeCannotBeChangedException(existingCluster))
          case _: RuntimeConfig.GceConfig => IO.raiseError(new NotImplementedError("GCE is not implemented"))
        }

      case None =>
        logger.debug("detected no cluster in maybeChangeMasterMachineType")
        IO.pure(UpdateResult(false, None))
    }
  }

  def maybeChangeMasterDiskSize(existingCluster: Cluster,
                                existingRuntimeConfig: RuntimeConfig,
                                targetMachineSize: Option[Int]): IO[UpdateResult] = {
    val updatedMasterDiskSizeOpt =
      getUpdatedValueIfChanged(Some(existingRuntimeConfig.diskSize), targetMachineSize)

    // Note: GCE allows you to increase a persistent disk, but not decrease. Throw an exception if the user tries to decrease their disk.
    val diskSizeIncreased = (newSize: Int) => existingRuntimeConfig.diskSize < newSize

    updatedMasterDiskSizeOpt match {
      case Some(updatedMasterDiskSize) if diskSizeIncreased(updatedMasterDiskSize) =>
        for {
          _ <- log.info(
            s"New target machine size present. Changing master disk size to $updatedMasterDiskSize GB for cluster ${existingCluster.projectNameString}..."
          )
          // Update the disk in Google
          _ <- clusterHelper.updateMasterDiskSize(existingCluster, updatedMasterDiskSize)
          // Update the DB
          now <- IO(Instant.now)
          _ <- RuntimeConfigQueries
            .updateDiskSize(RuntimeConfigId(existingCluster.runtimeConfigId), updatedMasterDiskSize, now)
            .transaction
        } yield UpdateResult(true, None)

      case Some(_) =>
        IO.raiseError(RuntimeDiskSizeCannotBeDecreasedException(existingCluster))

      case None =>
        IO.pure(UpdateResult(false, None))
    }
  }

  def deleteCluster(userInfo: UserInfo, googleProject: GoogleProject, clusterName: ClusterName)(
    implicit ev: ApplicativeAsk[IO, TraceId]
  ): IO[Unit] =
    for {
      //throws 404 if no permissions
      cluster <- getActiveClusterDetails(userInfo, googleProject, clusterName)

      //if you've got to here you at least have GetClusterDetails permissions so a 403 is appropriate if you can't actually destroy it
      _ <- checkClusterPermission(userInfo,
                                  DeleteCluster,
                                  cluster.internalId,
                                  RuntimeProjectAndName(cluster.googleProject, cluster.runtimeName),
                                  throw403 = true)

      _ <- internalDeleteCluster(userInfo.userEmail, cluster)
    } yield ()

  //NOTE: This function MUST ALWAYS complete ALL steps. i.e. if deleting thing1 fails, it must still proceed to delete thing2
  def internalDeleteCluster(userEmail: WorkbenchEmail,
                            cluster: Cluster)(implicit ev: ApplicativeAsk[IO, TraceId]): IO[Unit] =
    if (cluster.status.isDeletable) {
      for {
        // Delete the notebook service account key in Google, if present
        keyIdOpt <- clusterQuery.getServiceAccountKeyId(cluster.googleProject, cluster.runtimeName).transaction
        _ <- clusterHelper
          .removeServiceAccountKey(cluster.googleProject, cluster.serviceAccountInfo.notebookServiceAccount, keyIdOpt)
          .recoverWith {
            case NonFatal(e) =>
              log.error(e)(
                s"Error occurred removing service account key for ${cluster.googleProject} / ${cluster.runtimeName}"
              )
          }
        hasDataprocInfo = cluster.asyncRuntimeFields.isDefined
        // Delete the cluster in Google
        _ <- if (hasDataprocInfo) clusterHelper.deleteCluster(cluster) else IO.unit
        // Change the cluster status to Deleting in the database
        // Note this also changes the instance status to Deleting
        now <- IO(Instant.now)
        _ <- if (hasDataprocInfo) clusterQuery.markPendingDeletion(cluster.id, now).transaction
        else clusterQuery.completeDeletion(cluster.id, now).transaction
        _ <- if (hasDataprocInfo)
          IO.unit //When dataprocInfo is defined, there's async deletion from google happening and we don't want to delete sam resource immediately
        else
          authProvider
            .notifyClusterDeleted(cluster.internalId,
                                  cluster.auditInfo.creator,
                                  cluster.auditInfo.creator,
                                  cluster.googleProject,
                                  cluster.runtimeName)
      } yield ()
    } else if (cluster.status == RuntimeStatus.Creating) {
      IO.raiseError(RuntimeCannotBeDeletedException(cluster.googleProject, cluster.runtimeName))
    } else IO.unit

  def stopCluster(userInfo: UserInfo, googleProject: GoogleProject, clusterName: ClusterName)(
    implicit ev: ApplicativeAsk[IO, TraceId]
  ): IO[Unit] =
    for {
      //throws 404 if no permissions
      cluster <- getActiveClusterDetails(userInfo, googleProject, clusterName)

      //if you've got to here you at least have GetClusterDetails permissions so a 403 is appropriate if you can't actually stop it
      _ <- checkClusterPermission(userInfo,
                                  StopStartCluster,
                                  cluster.internalId,
                                  RuntimeProjectAndName(cluster.googleProject, cluster.runtimeName),
                                  throw403 = true)

      runtimeConfig <- RuntimeConfigQueries.getRuntimeConfig(RuntimeConfigId(cluster.runtimeConfigId)).transaction
      _ <- clusterHelper.stopCluster(cluster, runtimeConfig)
    } yield ()

  def internalStopCluster(cluster: Cluster)(implicit ev: ApplicativeAsk[IO, TraceId]): IO[Unit] =
    if (cluster.status.isStoppable) {
      for {
        // Flush the welder cache to disk
        _ <- if (cluster.welderEnabled) {
          welderDao
            .flushCache(cluster.googleProject, cluster.runtimeName)
            .handleErrorWith(e => log.error(e)(s"Failed to flush welder cache for ${cluster}"))
        } else IO.unit

        runtimeConfig <- RuntimeConfigQueries.getRuntimeConfig(RuntimeConfigId(cluster.runtimeConfigId)).transaction
        // Stop the cluster in Google
        _ <- clusterHelper.stopCluster(cluster, runtimeConfig)

        // Update the cluster status to Stopping
        now <- IO(Instant.now)
        _ <- clusterQuery.setToStopping(cluster.id, now).transaction
      } yield ()

    } else IO.raiseError(RuntimeCannotBeStoppedException(cluster.googleProject, cluster.runtimeName, cluster.status))

  def startCluster(userInfo: UserInfo, googleProject: GoogleProject, clusterName: ClusterName)(
    implicit ev: ApplicativeAsk[IO, TraceId]
  ): IO[Unit] =
    for {
      //throws 404 if no permissions
      cluster <- getActiveClusterDetails(userInfo, googleProject, clusterName)

      //if you've got to here you at least have GetClusterDetails permissions so a 403 is appropriate if you can't actually stop it
      _ <- checkClusterPermission(userInfo,
                                  StopStartCluster,
                                  cluster.internalId,
                                  RuntimeProjectAndName(cluster.googleProject, cluster.runtimeName),
                                  throw403 = true)

      now <- timer.clock.realTime(TimeUnit.MILLISECONDS)
      _ <- clusterHelper.startCluster(cluster, Instant.ofEpochMilli(now))
    } yield ()

  def listClusters(userInfo: UserInfo, params: LabelMap, googleProjectOpt: Option[GoogleProject] = None)(
    implicit ev: ApplicativeAsk[IO, TraceId]
  ): IO[Vector[ListRuntimeResponse]] =
    for {
      paramMap <- IO.fromEither(processListClustersParameters(params))
      clusters <- LeonardoServiceDbQueries.listClusters(paramMap._1, paramMap._2, googleProjectOpt).transaction
      samVisibleClusters <- authProvider
        .filterUserVisibleClusters(userInfo, clusters.map(c => (c.googleProject, c.internalId)))
    } yield {
      // Making the assumption that users will always be able to access clusters that they create
      // Fix for https://github.com/DataBiosphere/leonardo/issues/821
      clusters
        .filter(
          c => c.auditInfo.creator == userInfo.userEmail || samVisibleClusters.contains((c.googleProject, c.internalId))
        )
        .toVector
    }

  private[service] def calculateAutopauseThreshold(autopause: Option[Boolean], autopauseThreshold: Option[Int]): Int =
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
    clusterRequest: CreateRuntimeRequest
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

      // Validate GCS user script URIs. Http user scripts are validated when reading from json in LeoModel
      _ <- clusterRequest.jupyterUserScriptUri match {
        case Some(UserScriptPath.Gcs(gcsPath)) =>
          OptionT.liftF[IO, Unit](validateBucketObjectUri(userEmail, petToken, gcsPath.toUri))
        case _ => OptionT.pure[IO](())
      }
      _ <- clusterRequest.jupyterStartUserScriptUri match {
        case Some(UserScriptPath.Gcs(gcsPath)) =>
          OptionT.liftF[IO, Unit](validateBucketObjectUri(userEmail, petToken, gcsPath.toUri))
        case _ => OptionT.pure[IO](())
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

  private[service] def processListClustersParameters(
    params: LabelMap
  ): Either[ParseLabelsException, (LabelMap, Boolean)] =
    params.get(includeDeletedKey) match {
      case Some(includeDeletedValue) =>
        processLabelMap(params - includeDeletedKey).map(lm => (lm, includeDeletedValue.toBoolean))
      case None =>
        processLabelMap(params).map(lm => (lm, false))
    }

  private[service] def augmentClusterRequest(serviceAccountInfo: ServiceAccountInfo,
                                             googleProject: GoogleProject,
                                             clusterName: ClusterName,
                                             userEmail: WorkbenchEmail,
                                             clusterRequest: CreateRuntimeRequest,
                                             clusterImages: Set[ClusterImage]): CreateRuntimeRequest = {
    val userJupyterExt = clusterRequest.jupyterExtensionUri match {
      case Some(ext) => Map("notebookExtension" -> ext.toUri)
      case None      => Map.empty[String, String]
    }

    // add the userJupyterExt to the nbExtensions
    val updatedUserJupyterExtensionConfig = clusterRequest.userJupyterExtensionConfig match {
      case Some(config) => config.copy(nbExtensions = config.nbExtensions ++ userJupyterExt)
      case None         => UserJupyterExtensionConfig(userJupyterExt, Map.empty, Map.empty, Map.empty)
    }

    // transform Some(empty, empty, empty, empty) to None
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
                                        clusterRequest: CreateRuntimeRequest,
                                        clusterImages: Set[ClusterImage]): CreateRuntimeRequest = {
    // create a LabelMap of default labels
    // TODO
    val defaultLabels = Map.empty[String, String]
//      DefaultLabels(
//      clusterName,
//      googleProject,
//      creator,
//      serviceAccountInfo.clusterServiceAccount,
//      serviceAccountInfo.notebookServiceAccount,
//      clusterRequest.jupyterUserScriptUri,
//      clusterRequest.jupyterStartUserScriptUri,
//      clusterImages.map(_.imageType).filterNot(_ == Welder).headOption
//    ).toJson.asJsObject.fields.mapValues(labelValue => labelValue.convertTo[String])

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

  private[service] def getRuntimeImages(
    userEmail: WorkbenchEmail,
    googleProject: GoogleProject,
    clusterRequest: CreateRuntimeRequest
  )(implicit ev: ApplicativeAsk[IO, TraceId]): IO[Set[ClusterImage]] =
    for {
      now <- IO(Instant.now)
      traceId <- ev.ask
      petTokenOpt <- serviceAccountProvider.getAccessToken(userEmail, googleProject)
      // Try to autodetect the image
      autodetectedImageOpt <- clusterRequest.toolDockerImage.traverse { image =>
        dockerDAO.detectTool(image, petTokenOpt).flatMap {
          case None       => IO.raiseError(ImageNotFoundException(traceId, image))
          case Some(tool) => IO.pure(RuntimeImage(tool, image.imageUrl, now))
        }
      }
      // Figure out the tool image. Rules:
      // - if we were able to autodetect an image, use that
      // - else if a legacy jupyterDockerImage param was sent, use that
      // - else use the default jupyter image
      jupyterImageOpt = clusterRequest.jupyterDockerImage.map(i => RuntimeImage(Jupyter, i.imageUrl, now))
      defaultJupyterImage = RuntimeImage(Jupyter, imageConfig.jupyterImage, now)
      toolImage = autodetectedImageOpt orElse jupyterImageOpt getOrElse defaultJupyterImage
      // Figure out the welder image. Rules:
      // - If welder is enabled, we will use the client-supplied image if present, otherwise we will use a default.
      // - If welder is not enabled, we won't use any image.
      welderImageOpt = if (clusterRequest.enableWelder.getOrElse(false)) {
        val imageUrl = clusterRequest.welderDockerImage.map(_.imageUrl).getOrElse(imageConfig.welderImage)
        Some(RuntimeImage(Welder, imageUrl, now))
      } else None
    } yield Set(Some(toolImage), welderImageOpt).flatten

}

object LeonardoService {

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
  private[service] def processLabelMap(params: LabelMap): Either[ParseLabelsException, LabelMap] =
    params.get("_labels") match {
      case Some(extraLabels) =>
        val labels: List[Either[ParseLabelsException, LabelMap]] = extraLabels
          .split(',')
          .map { c =>
            c.split('=') match {
              case Array(key, value) => Map(key -> value).asRight[ParseLabelsException]
              case _                 => (ParseLabelsException(extraLabels)).asLeft[LabelMap]
            }
          }
          .toList

        implicit val mapAdd: Monoid[Map[String, String]] = Monoid.instance(Map.empty, (mp1, mp2) => mp1 ++ mp2)
        labels.combineAll
      case None => Right(params)
    }
}
