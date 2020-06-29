package org.broadinstitute.dsde.workbench.leonardo
package http
package service

import java.time.Instant
import java.util.UUID

import _root_.io.chrisdavenport.log4cats.Logger
import akka.actor.ActorSystem
import akka.http.scaladsl.model.StatusCodes
import cats.Monoid
import cats.data.OptionT
import cats.effect.{ContextShift, IO, Timer}
import cats.implicits._
import cats.mtl.ApplicativeAsk
import com.google.api.client.http.HttpResponseException
import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.dsde.workbench.google.GoogleStorageDAO
import org.broadinstitute.dsde.workbench.google2.MachineTypeName
import org.broadinstitute.dsde.workbench.leonardo.RuntimeImageType.{Jupyter, Proxy, Welder}
import org.broadinstitute.dsde.workbench.leonardo.RuntimeStatus.Stopped
import org.broadinstitute.dsde.workbench.leonardo.SamResource.RuntimeSamResource
import org.broadinstitute.dsde.workbench.leonardo.config._
import org.broadinstitute.dsde.workbench.leonardo.dao.google._
import org.broadinstitute.dsde.workbench.leonardo.dao.{DockerDAO, WelderDAO}
import org.broadinstitute.dsde.workbench.leonardo.db._
import org.broadinstitute.dsde.workbench.leonardo.http.service.LeonardoService._
import org.broadinstitute.dsde.workbench.leonardo.http.service.UpdateTransition._
import org.broadinstitute.dsde.workbench.leonardo.model.RuntimeAction._
import org.broadinstitute.dsde.workbench.leonardo.model.ProjectAction._
import org.broadinstitute.dsde.workbench.leonardo.model._
import org.broadinstitute.dsde.workbench.leonardo.monitor.LeoPubsubMessage
import org.broadinstitute.dsde.workbench.leonardo.monitor.LeoPubsubMessage.{
  CreateRuntimeMessage,
  DeleteRuntimeMessage,
  StartRuntimeMessage,
  StopRuntimeMessage
}
import org.broadinstitute.dsde.workbench.leonardo.util._
import org.broadinstitute.dsde.workbench.model.google._
import org.broadinstitute.dsde.workbench.model.{TraceId, UserInfo, WorkbenchEmail}
import org.broadinstitute.dsde.workbench.util.Retry

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._

case class AuthorizationError(email: Option[WorkbenchEmail] = None)
    extends LeoException(s"${email.map(e => s"'${e.value}'").getOrElse("Your account")} is unauthorized",
                         StatusCodes.Forbidden)

case class RuntimeNotFoundException(googleProject: GoogleProject, runtimeName: RuntimeName, msg: String)
    extends LeoException(s"Runtime ${googleProject.value}/${runtimeName.asString} not found. Details: ${msg}",
                         StatusCodes.NotFound)

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

final case class ImageNotFoundException(traceId: TraceId, image: ContainerImage)
    extends LeoException(s"${traceId} | Image ${image.imageUrl} not found", StatusCodes.NotFound)

final case class InvalidImage(traceId: TraceId, image: ContainerImage)
    extends LeoException(
      s"${traceId} | Image ${image.imageUrl} doesn't have JUPYTER_HOME or RSTUDIO_HOME environment variables defined. Make sure your custom image extends from one of the Terra base images.",
      StatusCodes.NotFound
    )

final case class CloudServiceNotSupportedException(cloudService: CloudService)
    extends LeoException(
      s"Cloud service ${cloudService.asString} is not support in /api/cluster routes. Please use /api/google/v1/runtime instead.",
      StatusCodes.Conflict
    )

case class UpdateResult(hasUpdateSucceded: Boolean, followupAction: Option[UpdateTransition])

sealed trait UpdateTransition extends Product with Serializable

object UpdateTransition {
  case class StopStartTransition(updateConfig: RuntimeConfig.DataprocConfig) extends UpdateTransition
  case class DeleteCreateTransition(updateConfig: RuntimeConfig.DataprocConfig) extends UpdateTransition
}

// Future runtime related APIs should be added to `RuntimeService`
class LeonardoService(
  protected val dataprocConfig: DataprocConfig,
  protected val imageConfig: ImageConfig,
  protected val welderDao: WelderDAO[IO],
  protected val proxyConfig: ProxyConfig,
  protected val swaggerConfig: SwaggerConfig,
  protected val autoFreezeConfig: AutoFreezeConfig,
  protected val zombieRuntimeMonitorConfig: ZombieRuntimeMonitorConfig,
  protected val welderConfig: WelderConfig,
  protected val petGoogleStorageDAO: String => GoogleStorageDAO,
  protected val authProvider: LeoAuthProvider[IO],
  protected val serviceAccountProvider: ServiceAccountProvider[IO],
  protected val bucketHelper: BucketHelper[IO],
  protected val dockerDAO: DockerDAO[IO],
  protected val publisherQueue: fs2.concurrent.Queue[IO, LeoPubsubMessage]
)(implicit val executionContext: ExecutionContext,
  implicit override val system: ActorSystem,
  log: Logger[IO],
  cs: ContextShift[IO],
  timer: Timer[IO],
  dbRef: DbReference[IO],
  runtimeInstances: RuntimeInstances[IO])
    extends LazyLogging
    with Retry {

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
                                       action: RuntimeAction,
                                       runtimeSamResource: RuntimeSamResource,
                                       runtimeProjectAndName: RuntimeProjectAndName,
                                       throw403: Boolean = false)(implicit ev: ApplicativeAsk[IO, TraceId]): IO[Unit] =
    for {
      traceId <- ev.ask
      hasPermission <- authProvider.hasRuntimePermission(runtimeSamResource,
                                                         userInfo,
                                                         action,
                                                         runtimeProjectAndName.googleProject)
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
                  RuntimeNotFoundException(runtimeProjectAndName.googleProject,
                                           runtimeProjectAndName.runtimeName,
                                           s"${runtimeSamResource} permission is required")
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
    runtimeName: RuntimeName,
    request: CreateRuntimeRequest
  )(implicit ev: ApplicativeAsk[IO, TraceId]): IO[CreateRuntimeResponse] =
    for {
      _ <- checkProjectPermission(userInfo, CreateRuntime, googleProject)
      // Grab the service accounts from serviceAccountProvider for use later
      clusterServiceAccount <- serviceAccountProvider
        .getClusterServiceAccount(userInfo, googleProject)

      petSA <- IO.fromEither(
        clusterServiceAccount.toRight(new Exception(s"no user ${userInfo.userEmail.value} PET SA found"))
      )
      clusterOpt <- clusterQuery.getActiveClusterByNameMinimal(googleProject, runtimeName).transaction

      cluster <- clusterOpt.fold(
        internalCreateCluster(userInfo.userEmail, petSA, googleProject, runtimeName, request)
      )(c => IO.raiseError(RuntimeAlreadyExistsException(googleProject, runtimeName, c.status)))

    } yield cluster

  private def internalCreateCluster(
    userEmail: WorkbenchEmail,
    serviceAccountInfo: WorkbenchEmail,
    googleProject: GoogleProject,
    runtimeName: RuntimeName,
    request: CreateRuntimeRequest
  )(implicit ev: ApplicativeAsk[IO, TraceId]): IO[CreateRuntimeResponse] =
    // Validate that the Jupyter extension URIs and Jupyter user script URI are valid URIs and reference real GCS objects
    // and if so, save the cluster creation request parameters in DB
    for {
      traceId <- ev.ask
      internalId <- IO(RuntimeSamResource(UUID.randomUUID().toString))
      // Get a pet token from Sam. If we can't get a token, we won't do validation but won't fail cluster creation.
      petToken <- serviceAccountProvider.getAccessToken(userEmail, googleProject).recoverWith {
        case e =>
          log.warn(e)(
            s"Could not acquire pet service account access token for user ${userEmail.value} in project $googleProject. " +
              s"Skipping validation of bucket objects in the cluster request."
          ) as None
      }
      clusterImages <- getRuntimeImages(petToken, userEmail, googleProject, request)
      augmentedClusterRequest = augmentCreateRuntimeRequest(serviceAccountInfo,
                                                            googleProject,
                                                            runtimeName,
                                                            userEmail,
                                                            request,
                                                            clusterImages)
      machineConfig = request.runtimeConfig
        .asInstanceOf[Option[RuntimeConfigRequest.DataprocConfig]]
        .map(_.toRuntimeConfigDataprocConfig(dataprocConfig.runtimeConfigDefaults))
        .getOrElse(dataprocConfig.runtimeConfigDefaults)
      now <- nowInstant
      autopauseThreshold = calculateAutopauseThreshold(request.autopause, request.autopauseThreshold, autoFreezeConfig)
      clusterScopes = if (request.scopes.isEmpty) dataprocConfig.defaultScopes else request.scopes
      initialClusterToSave = CreateRuntimeRequest.toRuntime(
        augmentedClusterRequest,
        internalId,
        userEmail,
        runtimeName,
        googleProject,
        serviceAccountInfo,
        proxyConfig.proxyUrlBase,
        autopauseThreshold,
        clusterScopes,
        clusterImages,
        now
      )
      _ <- log.info(
        s"[$traceId] will deploy the following images to cluster ${initialClusterToSave.projectNameString}: ${clusterImages}"
      )
      _ <- validateClusterRequestBucketObjectUri(petToken, userEmail, googleProject, augmentedClusterRequest) >> log
        .info(
          s"[$traceId] Attempting to notify the AuthProvider for creation of cluster ${initialClusterToSave.projectNameString}"
        )
      _ <- authProvider.notifyResourceCreated(internalId, userEmail, googleProject).handleErrorWith { t =>
        log.info(
          s"[$traceId] Failed to notify the AuthProvider for creation of cluster ${initialClusterToSave.projectNameString}"
        ) >> IO.raiseError(t)
      }
      saveCluster = SaveCluster(cluster = initialClusterToSave, runtimeConfig = machineConfig, now = now)
      cluster <- clusterQuery.save(saveCluster).transaction
      _ <- log.info(
        s"[$traceId] Inserted an initial record into the DB for cluster ${cluster.projectNameString}"
      )
      _ <- publisherQueue.enqueue1(CreateRuntimeMessage.fromRuntime(cluster, machineConfig, Some(traceId)))
    } yield CreateRuntimeResponse.fromRuntime(cluster, machineConfig)

  // throws 404 if nonexistent or no permissions
  def getActiveClusterDetails(userInfo: UserInfo, googleProject: GoogleProject, clusterName: RuntimeName)(
    implicit ev: ApplicativeAsk[IO, TraceId]
  ): IO[Runtime] =
    for {
      cluster <- internalGetActiveClusterDetails(googleProject, clusterName) //throws 404 if nonexistent
      _ <- checkClusterPermission(
        userInfo,
        GetRuntimeStatus,
        cluster.samResource,
        RuntimeProjectAndName(cluster.googleProject, cluster.runtimeName)
      ) //throws 404 if no auth
    } yield cluster

  // throws 404 if nonexistent or no permissions
  def getClusterAPI(userInfo: UserInfo, googleProject: GoogleProject, clusterName: RuntimeName)(
    implicit ev: ApplicativeAsk[IO, TraceId]
  ): IO[GetRuntimeResponse] =
    for {
      resp <- RuntimeServiceDbQueries
        .getRuntime(googleProject, clusterName)
        .transaction //throws 404 if nonexistent
      _ <- checkClusterPermission(userInfo,
                                  GetRuntimeStatus,
                                  resp.samResource,
                                  RuntimeProjectAndName(resp.googleProject, resp.clusterName)) //throws 404 if no auth
    } yield resp

  private def internalGetActiveClusterDetails(googleProject: GoogleProject, clusterName: RuntimeName): IO[Runtime] =
    clusterQuery.getActiveClusterByName(googleProject, clusterName).transaction.flatMap {
      case None          => IO.raiseError(RuntimeNotFoundException(googleProject, clusterName, "no runtime found in database"))
      case Some(cluster) => IO.pure(cluster)
    }

  private def getUpdatedValueIfChanged[A](existing: Option[A], updated: Option[A]): Option[A] =
    (existing, updated) match {
      case (None, Some(0)) =>
        None //An updated value of 0 is considered the same as None to prevent google APIs from complaining
      case (_, Some(x)) if updated != existing => Some(x)
      case _                                   => None
    }

  def maybeChangeMasterMachineType(existingCluster: Runtime,
                                   existingRuntimeConfig: RuntimeConfig,
                                   targetMachineType: Option[MachineTypeName],
                                   allowStop: Boolean,
                                   now: Instant)(implicit ev: ApplicativeAsk[IO, TraceId]): IO[UpdateResult] = {
    val updatedMasterMachineTypeOpt =
      getUpdatedValueIfChanged(Some(existingRuntimeConfig.machineType), targetMachineType)

    updatedMasterMachineTypeOpt match {
      // Note: instance must be stopped in order to change machine type
      case Some(updatedMasterMachineType) if existingCluster.status == Stopped =>
        for {
          _ <- CloudService.Dataproc.interpreter.updateMachineType(
            UpdateMachineTypeParams(existingCluster, updatedMasterMachineType, now)
          )
        } yield UpdateResult(true, None)

      case Some(updatedMasterMachineType) =>
        existingRuntimeConfig match {
          case x: RuntimeConfig.DataprocConfig =>
            logger.debug("in stop and update case of maybeChangeMasterMachineType")
            val updatedConfig = x.copy(masterMachineType = updatedMasterMachineType)

            if (allowStop) {
              val transition = StopStartTransition(updatedConfig)
              logger.debug(
                s"detected stop and update transition specified in request of maybeChangeMasterMachineType, ${transition}"
              )
              IO.pure(UpdateResult(false, Some(transition)))
            } else IO.raiseError(RuntimeMachineTypeCannotBeChangedException(existingCluster))
          case _ => IO.raiseError(new NotImplementedError("GCE is not implemented"))
        }

      case None =>
        logger.debug("detected no cluster in maybeChangeMasterMachineType")
        IO.pure(UpdateResult(false, None))
    }
  }

  def deleteCluster(userInfo: UserInfo, googleProject: GoogleProject, clusterName: RuntimeName)(
    implicit ev: ApplicativeAsk[IO, TraceId]
  ): IO[Unit] =
    for {
      //throws 404 if no permissions
      cluster <- getActiveClusterDetails(userInfo, googleProject, clusterName)

      //if you've got to here you at least have GetClusterDetails permissions so a 403 is appropriate if you can't actually destroy it
      _ <- checkClusterPermission(userInfo,
                                  DeleteRuntime,
                                  cluster.samResource,
                                  RuntimeProjectAndName(cluster.googleProject, cluster.runtimeName),
                                  throw403 = true)

      runtimeConfig <- RuntimeConfigQueries.getRuntimeConfig(cluster.runtimeConfigId).transaction
      _ <- if (runtimeConfig.cloudService == CloudService.Dataproc) IO.unit
      else IO.raiseError(CloudServiceNotSupportedException(runtimeConfig.cloudService))

      _ <- internalDeleteCluster(cluster)
    } yield ()

  //NOTE: This function MUST ALWAYS complete ALL steps. i.e. if deleting thing1 fails, it must still proceed to delete thing2
  def internalDeleteCluster(cluster: Runtime)(implicit ev: ApplicativeAsk[IO, TraceId]): IO[Unit] =
    if (cluster.status.isDeletable) {
      val hasDataprocInfo = cluster.asyncRuntimeFields.isDefined
      for {
        ctx <- ev.ask
        now <- nowInstant
        _ <- if (hasDataprocInfo)
          clusterQuery.updateClusterStatus(cluster.id, RuntimeStatus.PreDeleting, now).transaction >> publisherQueue
            .enqueue1(
              DeleteRuntimeMessage(cluster.id, false, Some(ctx))
            )
        else clusterQuery.completeDeletion(cluster.id, now).transaction
        _ <- labelQuery
          .save(cluster.id, LabelResourceType.Runtime, zombieRuntimeMonitorConfig.deletionConfirmationLabelKey, "false")
          .transaction
      } yield ()
    } else if (cluster.status == RuntimeStatus.Creating) {
      IO.raiseError(RuntimeCannotBeDeletedException(cluster.googleProject, cluster.runtimeName))
    } else IO.unit

  def stopCluster(userInfo: UserInfo, googleProject: GoogleProject, clusterName: RuntimeName)(
    implicit ev: ApplicativeAsk[IO, AppContext]
  ): IO[Unit] =
    for {
      ctx <- ev.ask
      //throws 404 if no permissions
      cluster <- getActiveClusterDetails(userInfo, googleProject, clusterName)
      _ <- ctx.span.traverse(s => IO(s.addAnnotation("Done getActiveClusterDetails")))

      //if you've got to here you at least have GetClusterDetails permissions so a 403 is appropriate if you can't actually stop it
      _ <- checkClusterPermission(userInfo,
                                  StopStartRuntime,
                                  cluster.samResource,
                                  RuntimeProjectAndName(cluster.googleProject, cluster.runtimeName),
                                  throw403 = true)
      _ <- ctx.span.traverse(s => IO(s.addAnnotation("Done check sam permission")))

      runtimeConfig <- RuntimeConfigQueries.getRuntimeConfig(cluster.runtimeConfigId).transaction
      _ <- if (runtimeConfig.cloudService == CloudService.Dataproc) IO.unit
      else IO.raiseError(CloudServiceNotSupportedException(runtimeConfig.cloudService))

      // throw 409 if the cluster is not stoppable
      _ <- if (cluster.status.isStoppable) IO.unit
      else
        IO.raiseError[Unit](RuntimeCannotBeStoppedException(cluster.googleProject, cluster.runtimeName, cluster.status))

      // Update the cluster status to Stopping in the DB
      now <- nowInstant
      _ <- clusterQuery.updateClusterStatus(cluster.id, RuntimeStatus.PreStopping, now).transaction

      _ <- ctx.span.traverse(s => IO(s.addAnnotation("before enqueue message")))
      // stop the runtime
      _ <- publisherQueue.enqueue1(StopRuntimeMessage(cluster.id, Some(ctx.traceId)))
    } yield ()

  def startCluster(userInfo: UserInfo, googleProject: GoogleProject, clusterName: RuntimeName)(
    implicit ev: ApplicativeAsk[IO, AppContext]
  ): IO[Unit] =
    for {
      ctx <- ev.ask
      //throws 404 if no permissions
      cluster <- getActiveClusterDetails(userInfo, googleProject, clusterName)
      _ <- ctx.span.traverse(s => IO(s.addAnnotation("Done getActiveClusterDetails")))

      //if you've got to here you at least have GetClusterDetails permissions so a 403 is appropriate if you can't actually stop it
      _ <- checkClusterPermission(userInfo,
                                  StopStartRuntime,
                                  cluster.samResource,
                                  RuntimeProjectAndName(cluster.googleProject, cluster.runtimeName),
                                  throw403 = true)
      _ <- ctx.span.traverse(s => IO(s.addAnnotation("Check stopStartCluster permission")))
      runtimeConfig <- RuntimeConfigQueries.getRuntimeConfig(cluster.runtimeConfigId).transaction
      _ <- if (runtimeConfig.cloudService == CloudService.Dataproc) IO.unit
      else IO.raiseError(CloudServiceNotSupportedException(runtimeConfig.cloudService))

      // throw 409 if the cluster is not startable
      _ <- if (cluster.status.isStartable) IO.unit
      else
        IO.raiseError[Unit](RuntimeCannotBeStartedException(cluster.googleProject, cluster.runtimeName, cluster.status))

      // start the runtime
      now <- nowInstant
      _ <- clusterQuery.updateClusterStatus(cluster.id, RuntimeStatus.PreStarting, now).transaction
      _ <- ctx.span.traverse(s => IO(s.addAnnotation("before enqueue message")))

      _ <- publisherQueue.enqueue1(StartRuntimeMessage(cluster.id, Some(ctx.traceId)))
    } yield ()

  def listClusters(userInfo: UserInfo, params: LabelMap, googleProjectOpt: Option[GoogleProject] = None)(
    implicit ev: ApplicativeAsk[IO, TraceId]
  ): IO[Vector[ListRuntimeResponse]] =
    for {
      paramMap <- IO.fromEither(processListParameters(params))
      clusters <- LeonardoServiceDbQueries.listClusters(paramMap._1, paramMap._2, googleProjectOpt).transaction
      samVisibleClusters <- authProvider
        .filterUserVisibleRuntimes(userInfo, clusters.map(c => (c.googleProject, c.samResource)))
    } yield {
      // Making the assumption that users will always be able to access clusters that they create
      // Fix for https://github.com/DataBiosphere/leonardo/issues/821
      clusters
        .filter(c =>
          c.auditInfo.creator == userInfo.userEmail || samVisibleClusters.contains((c.googleProject, c.samResource))
        )
        .toVector
    }

  private[service] def calculateAutopauseThreshold(autopause: Option[Boolean],
                                                   autopauseThreshold: Option[Int],
                                                   autoFreezeConfig: AutoFreezeConfig): Int =
    autopause match {
      case None =>
        autoFreezeConfig.autoFreezeAfter.toMinutes.toInt
      case Some(false) =>
        autoPauseOffValue
      case _ =>
        if (autopauseThreshold.isEmpty) autoFreezeConfig.autoFreezeAfter.toMinutes.toInt
        else Math.max(autoPauseOffValue, autopauseThreshold.get)
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

  private[service] def getRuntimeImages(
    petToken: Option[String],
    userEmail: WorkbenchEmail,
    googleProject: GoogleProject,
    clusterRequest: CreateRuntimeRequest
  )(implicit ev: ApplicativeAsk[IO, TraceId]): IO[Set[RuntimeImage]] =
    for {
      now <- nowInstant
      traceId <- ev.ask
      // Try to autodetect the image
      autodetectedImageOpt <- clusterRequest.toolDockerImage.traverse(image =>
        dockerDAO.detectTool(image, petToken).map(t => RuntimeImage(t, image.imageUrl, now))
      )
      // Figure out the tool image. Rules:
      // - if we were able to autodetect an image, use that
      // - else if a legacy jupyterDockerImage param was sent, use that
      // - else use the default jupyter image
      jupyterImageOpt = clusterRequest.jupyterDockerImage.map(i => RuntimeImage(Jupyter, i.imageUrl, now))
      defaultJupyterImage = RuntimeImage(Jupyter, imageConfig.jupyterImage.imageUrl, now)
      toolImage = autodetectedImageOpt orElse jupyterImageOpt getOrElse defaultJupyterImage
      // Figure out the welder image. Rules:
      // - If welder is enabled, we will use the client-supplied image if present, otherwise we will use a default.
      // - If welder is not enabled, we won't use any image.
      welderImageOpt = if (clusterRequest.enableWelder.getOrElse(false)) {
        val imageUrl = clusterRequest.welderDockerImage.map(_.imageUrl).getOrElse(imageConfig.welderImage.imageUrl)
        Some(RuntimeImage(Welder, imageUrl, now))
      } else None
      // Get the proxy image
      proxyImage = RuntimeImage(Proxy, imageConfig.proxyImage.imageUrl, now)
    } yield Set(Some(toolImage), welderImageOpt, Some(proxyImage)).flatten

  private[service] def validateClusterRequestBucketObjectUri(
    petTokenOpt: Option[String],
    userEmail: WorkbenchEmail,
    googleProject: GoogleProject,
    clusterRequest: CreateRuntimeRequest
  ): IO[Unit] = {
    val transformed = for {
      petToken <- OptionT.fromOption[IO](petTokenOpt)
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
}

object LeonardoService {
  private[service] val includeDeletedKey = "includeDeleted"
  private[service] val bucketPathMaxLength = 1024

  private[service] def processListParameters(
    params: LabelMap
  ): Either[ParseLabelsException, (LabelMap, Boolean)] =
    params.get(includeDeletedKey) match {
      case Some(includeDeletedValue) =>
        processLabelMap(params - includeDeletedKey).map(lm => (lm, includeDeletedValue.toBoolean))
      case None =>
        processLabelMap(params).map(lm => (lm, false))
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

  private[service] def augmentCreateRuntimeRequest(serviceAccountInfo: WorkbenchEmail,
                                                   googleProject: GoogleProject,
                                                   clusterName: RuntimeName,
                                                   userEmail: WorkbenchEmail,
                                                   request: CreateRuntimeRequest,
                                                   clusterImages: Set[RuntimeImage]): CreateRuntimeRequest =
    addClusterLabels(serviceAccountInfo, googleProject, clusterName, userEmail, request, clusterImages)

  private[service] def calculateAutopauseThreshold(autopause: Option[Boolean],
                                                   autopauseThreshold: Option[Int],
                                                   autoFreezeConfig: AutoFreezeConfig): Int =
    autopause match {
      case None =>
        autoFreezeConfig.autoFreezeAfter.toMinutes.toInt
      case Some(false) =>
        autoPauseOffValue
      case _ =>
        if (autopauseThreshold.isEmpty) autoFreezeConfig.autoFreezeAfter.toMinutes.toInt
        else Math.max(autoPauseOffValue, autopauseThreshold.get)
    }

  private[service] def addClusterLabels(serviceAccountInfo: WorkbenchEmail,
                                        googleProject: GoogleProject,
                                        clusterName: RuntimeName,
                                        creator: WorkbenchEmail,
                                        request: CreateRuntimeRequest,
                                        clusterImages: Set[RuntimeImage]): CreateRuntimeRequest = {
    // create a LabelMap of default labels
    val defaultLabels = DefaultRuntimeLabels(
      clusterName,
      googleProject,
      creator,
      serviceAccountInfo,
      request.jupyterUserScriptUri,
      request.jupyterStartUserScriptUri,
      clusterImages.map(_.imageType).filterNot(_ == Welder).headOption
    )

    val defaultLabelsMap = defaultLabels.toMap

    // combine default and given labels and add labels for extensions
    val allLabels = request.labels ++ defaultLabelsMap ++
      request.userJupyterExtensionConfig.map(_.asLabels).getOrElse(Map.empty)

    // check the labels do not contain forbidden keys
    if (allLabels.contains(includeDeletedKey))
      throw IllegalLabelKeyException(includeDeletedKey)
    else
      request
        .copy(labels = allLabels)
  }
}
