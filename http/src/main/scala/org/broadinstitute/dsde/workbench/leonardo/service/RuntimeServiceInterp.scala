package org.broadinstitute.dsde.workbench.leonardo
package http
package service

import java.time.Instant
import java.util.UUID

import akka.http.scaladsl.model.StatusCodes
import cats.Parallel
import cats.effect.Async
import cats.implicits._
import cats.mtl.ApplicativeAsk
import com.google.auth.oauth2.{AccessToken, GoogleCredentials}
import com.google.cloud.BaseServiceException
import io.chrisdavenport.log4cats.StructuredLogger
import org.broadinstitute.dsde.workbench.google2.util.RetryPredicates
import org.broadinstitute.dsde.workbench.google2.{DiskName, GcsBlobName, GoogleStorageService, MachineTypeName}
import org.broadinstitute.dsde.workbench.leonardo.RuntimeImageType.{Jupyter, Proxy, Welder}
import org.broadinstitute.dsde.workbench.leonardo.SamResource.{PersistentDiskSamResource, RuntimeSamResource}
import org.broadinstitute.dsde.workbench.leonardo.config._
import org.broadinstitute.dsde.workbench.leonardo.dao.DockerDAO
import org.broadinstitute.dsde.workbench.leonardo.db._
import org.broadinstitute.dsde.workbench.leonardo.http.api.{
  ListRuntimeResponse2,
  UpdateRuntimeConfigRequest,
  UpdateRuntimeRequest
}
import org.broadinstitute.dsde.workbench.leonardo.http.service.LeonardoService._
import org.broadinstitute.dsde.workbench.leonardo.http.service.RuntimeServiceInterp._
import org.broadinstitute.dsde.workbench.leonardo.model.ProjectAction._
import org.broadinstitute.dsde.workbench.leonardo.model.RuntimeAction._
import org.broadinstitute.dsde.workbench.leonardo.model._
import org.broadinstitute.dsde.workbench.leonardo.monitor.{LeoPubsubMessage, RuntimePatchDetails}
import org.broadinstitute.dsde.workbench.leonardo.monitor.LeoPubsubMessage._
import org.broadinstitute.dsde.workbench.model.google.GoogleProject
import org.broadinstitute.dsde.workbench.model.{google, TraceId, UserInfo, WorkbenchEmail}
import org.broadinstitute.dsde.workbench.openTelemetry.OpenTelemetryMetrics

import scala.concurrent.ExecutionContext

class RuntimeServiceInterp[F[_]: Parallel](config: RuntimeServiceConfig,
                                           diskConfig: PersistentDiskConfig,
                                           authProvider: LeoAuthProvider[F],
                                           serviceAccountProvider: ServiceAccountProvider[F],
                                           dockerDAO: DockerDAO[F],
                                           googleStorageService: GoogleStorageService[F],
                                           publisherQueue: fs2.concurrent.Queue[F, LeoPubsubMessage])(
  implicit F: Async[F],
  log: StructuredLogger[F],
  dbReference: DbReference[F],
  ec: ExecutionContext,
  metrics: OpenTelemetryMetrics[F]
) extends RuntimeService[F] {

  override def createRuntime(
    userInfo: UserInfo,
    googleProject: GoogleProject,
    runtimeName: RuntimeName,
    req: CreateRuntime2Request
  )(implicit as: ApplicativeAsk[F, AppContext]): F[Unit] =
    for {
      context <- as.ask
      hasPermission <- authProvider.hasProjectPermission(userInfo, CreateRuntime, googleProject)
      _ <- context.span.traverse(s => F.delay(s.addAnnotation("Done Sam call for cluster permission")))
      _ <- if (hasPermission) F.unit else F.raiseError[Unit](AuthorizationError(Some(userInfo.userEmail)))
      // Grab the service accounts from serviceAccountProvider for use later
      runtimeServiceAccountOpt <- serviceAccountProvider
        .getClusterServiceAccount(userInfo, googleProject)
      _ <- context.span.traverse(s => F.delay(s.addAnnotation("Done Sam call for getClusterServiceAccount")))
      petSA <- F.fromEither(
        runtimeServiceAccountOpt.toRight(new Exception(s"user ${userInfo.userEmail.value} doesn't have a PET SA"))
      )

      runtimeOpt <- RuntimeServiceDbQueries.getStatusByName(googleProject, runtimeName).transaction
      _ <- context.span.traverse(s => F.delay(s.addAnnotation("Done DB query for active cluster")))
      _ <- runtimeOpt match {
        case Some(status) => F.raiseError[Unit](RuntimeAlreadyExistsException(googleProject, runtimeName, status))
        case None =>
          for {
            samResource <- F.delay(RuntimeSamResource(UUID.randomUUID().toString))
            petToken <- serviceAccountProvider.getAccessToken(userInfo.userEmail, googleProject).recoverWith {
              case e =>
                log.warn(e)(
                  s"Could not acquire pet service account access token for user ${userInfo.userEmail.value} in project $googleProject. " +
                    s"Skipping validation of bucket objects in the runtime request."
                ) as None
            }
            _ <- context.span.traverse(s => F.delay(s.addAnnotation("Done Sam getAccessToken")))
            runtimeImages <- getRuntimeImages(petToken, context.now, req.toolDockerImage, req.welderDockerImage)
            _ <- context.span.traverse(s => F.delay(s.addAnnotation("Done get runtime images")))
            runtimeConfig <- req.runtimeConfig
              .fold[F[RuntimeConfig]](F.pure(config.gceConfig.runtimeConfigDefaults)) { // default to gce if no runtime specific config is provided
                c =>
                  c match {
                    case gce: RuntimeConfigRequest.GceConfig =>
                      F.pure(
                        RuntimeConfig.GceConfig(
                          gce.machineType.getOrElse(config.gceConfig.runtimeConfigDefaults.machineType),
                          gce.diskSize.getOrElse(config.gceConfig.runtimeConfigDefaults.diskSize),
                          config.gceConfig.runtimeConfigDefaults.bootDiskSize
                        ): RuntimeConfig
                      )
                    case dataproc: RuntimeConfigRequest.DataprocConfig =>
                      F.pure(
                        dataproc
                          .toRuntimeConfigDataprocConfig(config.dataprocConfig.runtimeConfigDefaults): RuntimeConfig
                      )
                    case gce: RuntimeConfigRequest.GceWithPdConfig =>
                      RuntimeServiceInterp
                        .processPersistentDiskRequest(gce.persistentDisk,
                                                      googleProject,
                                                      userInfo,
                                                      petSA,
                                                      FormattedBy.GCE,
                                                      authProvider,
                                                      diskConfig)
                        .map(diskResult =>
                          RuntimeConfig.GceWithPdConfig(
                            gce.machineType.getOrElse(config.gceConfig.runtimeConfigDefaults.machineType),
                            Some(diskResult.disk.id),
                            config.gceConfig.runtimeConfigDefaults.bootDiskSize.get // .get here is okay. We've verified the data in Config.scala
                          ): RuntimeConfig
                        )
                  }
              }
            runtime <- F.fromEither(
              convertToRuntime(userInfo,
                               petSA,
                               googleProject,
                               runtimeName,
                               samResource,
                               runtimeImages,
                               config,
                               req,
                               context.now)
            )
            gcsObjectUrisToValidate = runtime.userJupyterExtensionConfig
              .map(config =>
                (config.nbExtensions.values ++ config.serverExtensions.values ++ config.combinedExtensions.values)
                  .filter(_.startsWith("gs://"))
                  .toList
              )
              .getOrElse(List.empty) ++ req.jupyterUserScriptUri.map(_.asString) ++ req.jupyterStartUserScriptUri.map(
              _.asString
            )
            _ <- petToken.traverse(t =>
              gcsObjectUrisToValidate
                .parTraverse(s => validateBucketObjectUri(userInfo.userEmail, t, s, context.traceId))
            )
            _ <- context.span.traverse(s => F.delay(s.addAnnotation("Done validating buckets")))
            _ <- authProvider
              .notifyResourceCreated(samResource, userInfo.userEmail, googleProject)
              .handleErrorWith { t =>
                log.error(t)(
                  s"[${context.traceId}] Failed to notify the AuthProvider for creation of runtime ${runtime.projectNameString}"
                ) >> F.raiseError(t)
              }
            _ <- context.span.traverse(s => F.delay(s.addAnnotation("Done Sam notifyClusterCreated")))

            saveRuntime = SaveCluster(cluster = runtime, runtimeConfig = runtimeConfig, now = context.now)
            runtime <- clusterQuery.save(saveRuntime).transaction
            _ <- publisherQueue.enqueue1(
              CreateRuntimeMessage.fromRuntime(runtime, runtimeConfig, Some(context.traceId))
            )
          } yield ()
      }
    } yield ()

  override def getRuntime(userInfo: UserInfo, googleProject: GoogleProject, runtimeName: RuntimeName)(
    implicit as: ApplicativeAsk[F, AppContext]
  ): F[GetRuntimeResponse] =
    for {
      // throws 404 if not existent
      resp <- RuntimeServiceDbQueries.getRuntime(googleProject, runtimeName).transaction
      // throw 404 if no GetClusterStatus permission
      hasPermission <- authProvider.hasRuntimePermission(resp.samResource, userInfo, GetRuntimeStatus, googleProject)
      _ <- if (hasPermission) F.unit
      else F.raiseError[Unit](RuntimeNotFoundException(googleProject, runtimeName, "permission denied"))
    } yield resp

  override def listRuntimes(userInfo: UserInfo, googleProject: Option[GoogleProject], params: Map[String, String])(
    implicit as: ApplicativeAsk[F, AppContext]
  ): F[Vector[ListRuntimeResponse2]] =
    for {
      ctx <- as.ask
      paramMap <- F.fromEither(processListParameters(params))
      runtimes <- RuntimeServiceDbQueries.listRuntimes(paramMap._1, paramMap._2, googleProject).transaction
      _ <- ctx.span.traverse(s => F.delay(s.addAnnotation("DB | Done listRuntime db query")))
      samVisibleRuntimes <- authProvider
        .filterUserVisibleRuntimes(userInfo, runtimes.map(r => (r.googleProject, r.samResource)))
      _ <- ctx.span.traverse(s => F.delay(s.addAnnotation("Sam | Done visible runtimes")))
    } yield {
      // Making the assumption that users will always be able to access runtimes that they create
      // Fix for https://github.com/DataBiosphere/leonardo/issues/821
      runtimes
        .filter(c =>
          c.auditInfo.creator == userInfo.userEmail || samVisibleRuntimes.contains((c.googleProject, c.samResource))
        )
        .toVector
    }

  override def deleteRuntime(req: DeleteRuntimeRequest)(
    implicit ev: ApplicativeAsk[F, AppContext]
  ): F[Unit] =
    for {
      ctx <- ev.ask
      // throw 404 if not existent
      runtimeOpt <- clusterQuery.getActiveClusterByNameMinimal(req.googleProject, req.runtimeName).transaction
      _ <- ctx.span.traverse(s => F.delay(s.addAnnotation("DB | Done getActiveClusterByNameMinimal")))
      runtime <- runtimeOpt.fold(
        F.raiseError[Runtime](RuntimeNotFoundException(req.googleProject, req.runtimeName, "no record in database"))
      )(F.pure)
      // throw 404 if no GetClusterStatus permission
      // Note: the general pattern is to 404 (e.g. pretend the runtime doesn't exist) if the caller doesn't have
      // GetClusterStatus permission. We return 403 if the user can view the runtime but can't perform some other action.
      listOfPermissions <- authProvider.getRuntimeActionsWithProjectFallback(req.googleProject,
                                                                             runtime.samResource,
                                                                             req.userInfo)
      hasStatusPermission = listOfPermissions.toSet.contains(RuntimeAction.GetRuntimeStatus)

      _ <- ctx.span.traverse(s => F.delay(s.addAnnotation("Sam | Done get list of allowed actions")))

      _ <- if (hasStatusPermission) F.unit
      else
        F.raiseError[Unit](
          RuntimeNotFoundException(req.googleProject, req.runtimeName, "no active runtime record in database")
        )

      // throw 403 if no DeleteCluster permission

      hasDeletePermission = listOfPermissions.toSet.contains(RuntimeAction.DeleteRuntime)

      _ <- if (hasDeletePermission) F.unit else F.raiseError[Unit](AuthorizationError(Some(req.userInfo.userEmail)))
      // throw 409 if the cluster is not deletable
      _ <- if (runtime.status.isDeletable) F.unit
      else F.raiseError[Unit](RuntimeCannotBeDeletedException(runtime.googleProject, runtime.runtimeName))
      // delete the runtime

      _ <- if (runtime.asyncRuntimeFields.isDefined) {
        clusterQuery.updateClusterStatus(runtime.id, RuntimeStatus.PreDeleting, ctx.now).transaction >> publisherQueue
          .enqueue1(
            DeleteRuntimeMessage(runtime.id, req.deleteDisk, Some(ctx.traceId))
          )
      } else {
        clusterQuery.completeDeletion(runtime.id, ctx.now).transaction.void >> authProvider.notifyResourceDeleted(
          runtime.samResource,
          runtime.auditInfo.creator,
          runtime.auditInfo.creator,
          runtime.googleProject
        )
      }
      _ <- labelQuery
        .save(runtime.id,
              LabelResourceType.Runtime,
              config.zombieRuntimeMonitorConfig.deletionConfirmationLabelKey,
              "false")
        .transaction
    } yield ()

  def stopRuntime(userInfo: UserInfo, googleProject: GoogleProject, runtimeName: RuntimeName)(
    implicit as: ApplicativeAsk[F, AppContext]
  ): F[Unit] =
    for {
      ctx <- as.ask
      // throw 404 if not existent
      runtimeOpt <- clusterQuery.getActiveClusterByNameMinimal(googleProject, runtimeName).transaction
      _ <- ctx.span.traverse(s => F.delay(s.addAnnotation("Finish query for active runtime")))
      runtime <- runtimeOpt.fold(
        F.raiseError[Runtime](
          RuntimeNotFoundException(googleProject, runtimeName, "no active runtime found in database")
        )
      )(F.pure)
      // throw 404 if no GetClusterStatus permission
      // Note: the general pattern is to 404 (e.g. pretend the runtime doesn't exist) if the caller doesn't have
      // GetClusterStatus permission. We return 403 if the user can view the runtime but can't perform some other action.

      listOfPermissions <- authProvider.getRuntimeActions(runtime.samResource, userInfo)

      hasStatusPermission = listOfPermissions.toSet.contains(RuntimeAction.GetRuntimeStatus)

      _ <- ctx.span.traverse(s => F.delay(s.addAnnotation("Sam | Done get list of allowed actions")))

      _ <- if (hasStatusPermission) F.unit
      else
        F.raiseError[Unit](
          RuntimeNotFoundException(googleProject,
                                   runtimeName,
                                   "GetRuntimeStatus permission is required for stopRuntime")
        )

      // throw 403 if no StopStartCluster permission
      hasStopPermission = listOfPermissions.toSet.contains(RuntimeAction.StopStartRuntime)

      _ <- if (hasStopPermission) F.unit else F.raiseError[Unit](AuthorizationError(Some(userInfo.userEmail)))
      // throw 409 if the cluster is not stoppable
      _ <- if (runtime.status.isStoppable) F.unit
      else
        F.raiseError[Unit](RuntimeCannotBeStoppedException(runtime.googleProject, runtime.runtimeName, runtime.status))
      // stop the runtime
      _ <- clusterQuery.updateClusterStatus(runtime.id, RuntimeStatus.PreStopping, ctx.now).transaction
      _ <- publisherQueue.enqueue1(StopRuntimeMessage(runtime.id, Some(ctx.traceId)))
    } yield ()

  def startRuntime(userInfo: UserInfo, googleProject: GoogleProject, runtimeName: RuntimeName)(
    implicit as: ApplicativeAsk[F, AppContext]
  ): F[Unit] =
    for {
      ctx <- as.ask
      // throw 404 if not existent
      runtimeOpt <- clusterQuery.getActiveClusterByNameMinimal(googleProject, runtimeName).transaction
      _ <- ctx.span.traverse(s => F.delay(s.addAnnotation("Done query for active runtime")))
      runtime <- runtimeOpt.fold(
        F.raiseError[Runtime](RuntimeNotFoundException(googleProject, runtimeName, "no record in database"))
      )(F.pure)
      // throw 404 if no GetClusterStatus permission
      // Note: the general pattern is to 404 (e.g. pretend the runtime doesn't exist) if the caller doesn't have
      // GetClusterStatus permission. We return 403 if the user can view the runtime but can't perform some other action.

      listOfPermissions <- authProvider.getRuntimeActions(runtime.samResource, userInfo)

      hasStatusPermission = listOfPermissions.toSet.contains(RuntimeAction.GetRuntimeStatus)

      _ <- if (hasStatusPermission) F.unit
      else
        F.raiseError[Unit](
          RuntimeNotFoundException(googleProject, runtimeName, "GetRuntimeStatus permission is required")
        )

      _ <- ctx.span.traverse(s => F.delay(s.addAnnotation("Sam | Done get list of allowed actions")))

      hasStartPermission = listOfPermissions.contains(RuntimeAction.StopStartRuntime)
      // throw 403 if no StopStartCluster permission
      _ <- if (hasStartPermission) F.unit else F.raiseError[Unit](AuthorizationError(Some(userInfo.userEmail)))

      // throw 409 if the cluster is not startable
      _ <- if (runtime.status.isStartable) F.unit
      else
        F.raiseError[Unit](RuntimeCannotBeStartedException(runtime.googleProject, runtime.runtimeName, runtime.status))
      // start the runtime
      ctx <- as.ask
      _ <- clusterQuery.updateClusterStatus(runtime.id, RuntimeStatus.PreStarting, ctx.now).transaction
      _ <- publisherQueue.enqueue1(StartRuntimeMessage(runtime.id, Some(ctx.traceId)))
    } yield ()

  override def updateRuntime(
    userInfo: UserInfo,
    googleProject: GoogleProject,
    runtimeName: RuntimeName,
    req: UpdateRuntimeRequest
  )(implicit as: ApplicativeAsk[F, AppContext]): F[Unit] =
    for {
      ctx <- as.ask
      // throw 404 if not existent
      runtimeOpt <- clusterQuery.getActiveClusterByNameMinimal(googleProject, runtimeName).transaction
      runtime <- runtimeOpt.fold(
        F.raiseError[Runtime](RuntimeNotFoundException(googleProject, runtimeName, "no record in database"))
      )(F.pure)
      // throw 404 if no GetClusterStatus permission
      // Note: the general pattern is to 404 (e.g. pretend the runtime doesn't exist) if the caller doesn't have
      // GetClusterStatus permission. We return 403 if the user can view the runtime but can't perform some other action.

      listOfPermissions <- authProvider.getRuntimeActions(runtime.samResource, userInfo)

      hasStatusPermission = listOfPermissions.toSet.contains(RuntimeAction.GetRuntimeStatus)

      _ <- if (hasStatusPermission) F.unit
      else
        F.raiseError[Unit](
          RuntimeNotFoundException(googleProject,
                                   runtimeName,
                                   "GetRuntimeStatus permission is required for update runtime")
        )

      // throw 403 if no ModifyCluster permission
      hasModifyPermission = listOfPermissions.contains(RuntimeAction.ModifyRuntime)

      _ <- if (hasModifyPermission) F.unit else F.raiseError[Unit](AuthorizationError(Some(userInfo.userEmail)))
      // throw 409 if the cluster is not updatable
      _ <- if (runtime.status.isUpdatable) F.unit
      else
        F.raiseError[Unit](RuntimeCannotBeUpdatedException(runtime.projectNameString, runtime.status))
      runtimeConfig <- RuntimeConfigQueries.getRuntimeConfig(runtime.runtimeConfigId).transaction
      // Updating autopause is just a DB update, so we can do it here instead of sending a PubSub message
      updatedAutopauseThreshold = calculateAutopauseThreshold(req.updateAutopauseEnabled,
                                                              req.updateAutopauseThreshold.map(_.toMinutes.toInt),
                                                              config.autoFreezeConfig)
      _ <- if (updatedAutopauseThreshold != runtime.autopauseThreshold)
        clusterQuery.updateAutopauseThreshold(runtime.id, updatedAutopauseThreshold, ctx.now).transaction.void
      else Async[F].unit
      // Updating the runtime config will potentially generate a PubSub message
      _ <- req.updatedRuntimeConfig.traverse_(update =>
        processUpdateRuntimeConfigRequest(update, req.allowStop, runtime, runtimeConfig, ctx.traceId)
      )
    } yield ()

  private[service] def getRuntimeImages(
    petToken: Option[String],
    now: Instant,
    toolDockerImage: Option[ContainerImage],
    welderDockerImage: Option[ContainerImage]
  )(implicit ev: ApplicativeAsk[F, TraceId]): F[Set[RuntimeImage]] =
    for {
      // Try to autodetect the image
      autodetectedImageOpt <- toolDockerImage.traverse(image =>
        dockerDAO.detectTool(image, petToken).map(t => RuntimeImage(t, image.imageUrl, now))
      )
      // Figure out the tool image. Rules:
      // - if we were able to autodetect an image, use that
      // - else use the default jupyter image
      defaultJupyterImage = RuntimeImage(Jupyter, config.imageConfig.jupyterImage.imageUrl, now)
      toolImage = autodetectedImageOpt getOrElse defaultJupyterImage
      // Figure out the welder image. Rules:
      // - If welder is enabled, we will use the client-supplied image if present, otherwise we will use a default.
      // - If welder is not enabled, we won't use any image.
      welderImage = RuntimeImage(
        Welder,
        welderDockerImage.map(_.imageUrl).getOrElse(config.imageConfig.welderImage.imageUrl),
        now
      )
      // Get the proxy image
      proxyImage = RuntimeImage(Proxy, config.imageConfig.proxyImage.imageUrl, now)
    } yield Set(toolImage, welderImage, proxyImage)

  private[service] def validateBucketObjectUri(userEmail: WorkbenchEmail,
                                               userToken: String,
                                               gcsUri: String,
                                               traceId: TraceId): F[Unit] = {
    val gcsUriOpt = google.parseGcsPath(gcsUri)
    gcsUriOpt match {
      case Left(_)                                                      => F.raiseError(BucketObjectException(gcsUri))
      case Right(gcsPath) if gcsPath.toUri.length > bucketPathMaxLength => F.raiseError(BucketObjectException(gcsUri))
      case Right(gcsPath)                                               =>
        // Retry 401s from Google here because they can be thrown spuriously with valid credentials.
        // See https://github.com/DataBiosphere/leonardo/issues/460
        // Note GoogleStorageDAO already retries 500 and other errors internally, so we just need to catch 401s here.
        // We might think about moving the retry-on-401 logic inside GoogleStorageDAO.
        val accessToken = new AccessToken(userToken, null) // we currently don't get expiration time from sam
        val credentials = GoogleCredentials.create(accessToken)
        val retryPolicy = RetryPredicates.retryConfigWithPredicates(
          RetryPredicates.standardRetryPredicate,
          RetryPredicates.whenStatusCode(401)
        )

        val res = for {
          blob <- googleStorageService
            .getBlob(gcsPath.bucketName,
                     GcsBlobName(gcsPath.objectName.value),
                     credential = Some(credentials),
                     Some(traceId),
                     retryPolicy)
            .compile
            .last
          _ <- if (blob.isDefined) F.unit else F.raiseError[Unit](BucketObjectException(gcsUri))
        } yield ()

        res.recoverWith {
          case e: BaseServiceException if e.getCode == StatusCodes.Forbidden.intValue =>
            log.error(e)(
              s"User ${userEmail.value}'s PET account does not have access to ${gcsPath.bucketName} / ${gcsPath.objectName}"
            ) >> F.raiseError(BucketObjectAccessException(userEmail, gcsPath))
          case e: BaseServiceException if e.getCode == 401 =>
            log.warn(e)(s"Could not validate object [${gcsUri}] as user [${userEmail.value}]")
        }
    }
  }

  /**
   * This method validates an UpdateRuntimeConfigRequest against the current state of the runtime,
   * and potentially sends a PubSub message (or throws an error). The PubSub receiver does not
   * do validation; it is assumed all validation happens here.
   */
  private[service] def processUpdateRuntimeConfigRequest(request: UpdateRuntimeConfigRequest,
                                                         allowStop: Boolean,
                                                         runtime: Runtime,
                                                         runtimeConfig: RuntimeConfig,
                                                         traceId: TraceId): F[Unit] =
    for {

      msg <- (runtimeConfig, request) match {
        case (gceConfig @ RuntimeConfig.GceConfig(_, _, _), req @ UpdateRuntimeConfigRequest.GceConfig(_, _)) =>
          processUpdateGceConfigRequest(req, allowStop, runtime, gceConfig, traceId)
        case (dataprocConfig @ RuntimeConfig.DataprocConfig(_, _, _, _, _, _, _, _),
              req @ UpdateRuntimeConfigRequest.DataprocConfig(_, _, _, _)) =>
          processUpdateDataprocConfigRequest(req, allowStop, runtime, dataprocConfig, traceId)

        case _ =>
          Async[F].raiseError[Option[UpdateRuntimeMessage]](
            WrongCloudServiceException(runtimeConfig.cloudService, request.cloudService, traceId)
          )
      }
      _ <- msg.traverse_ { m =>
        if (m.stopToUpdateMachineType) {
          val patchDetails = RuntimePatchDetails(runtime.id, RuntimeStatus.Stopped)
          patchQuery.save(patchDetails, m.newMachineType).transaction.void >> metrics.incrementCounter(
            "patchStopToUpdateMachineType"
          )
        } else F.unit
      }
    } yield ()

  private[service] def processUpdateGceConfigRequest(req: UpdateRuntimeConfigRequest.GceConfig,
                                                     allowStop: Boolean,
                                                     runtime: Runtime,
                                                     gceConfig: RuntimeConfig.GceConfig,
                                                     traceId: TraceId): F[Option[UpdateRuntimeMessage]] =
    for {
      // should machine type be updated?
      targetMachineType <- traverseIfChanged(req.updatedMachineType, gceConfig.machineType) { mt =>
        if (runtime.status == RuntimeStatus.Stopped)
          Async[F].pure((mt, false))
        else if (!allowStop)
          Async[F].raiseError[(MachineTypeName, Boolean)](RuntimeMachineTypeCannotBeChangedException(runtime))
        else Async[F].pure((mt, true))
      }
      // should disk size be updated?
      targetDiskSize <- traverseIfChanged(req.updatedDiskSize, gceConfig.diskSize) { d =>
        if (d.gb < gceConfig.diskSize.gb)
          Async[F].raiseError[DiskSize](RuntimeDiskSizeCannotBeDecreasedException(runtime))
        else
          Async[F].pure(d)
      }
      // if either of the above is defined, send a PubSub message
      msg <- (targetMachineType orElse targetDiskSize).traverse { _ =>
        val message = UpdateRuntimeMessage(runtime.id,
                                           targetMachineType.map(_._1),
                                           targetMachineType.map(_._2).getOrElse(false),
                                           targetDiskSize,
                                           None,
                                           None,
                                           Some(traceId))
        publisherQueue.enqueue1(message).as(message)
      }
    } yield msg

  private[service] def processUpdateDataprocConfigRequest(req: UpdateRuntimeConfigRequest.DataprocConfig,
                                                          allowStop: Boolean,
                                                          runtime: Runtime,
                                                          dataprocConfig: RuntimeConfig.DataprocConfig,
                                                          traceId: TraceId): F[Option[UpdateRuntimeMessage]] =
    for {
      // should num workers be updated?
      targetNumWorkers <- traverseIfChanged(req.updatedNumberOfWorkers, dataprocConfig.numberOfWorkers)(Async[F].pure)
      // should num preemptibles be updated?
      targetNumPreemptibles <- traverseIfChanged(req.updatedNumberOfPreemptibleWorkers,
                                                 dataprocConfig.numberOfPreemptibleWorkers.getOrElse(0))(Async[F].pure)
      // should master machine type be updated?
      targetMasterMachineType <- traverseIfChanged(req.updatedMasterMachineType, dataprocConfig.masterMachineType) {
        mt =>
          if (targetNumWorkers.isDefined || targetNumPreemptibles.isDefined)
            Async[F].raiseError[(MachineTypeName, Boolean)](
              RuntimeCannotBeUpdatedException(
                runtime.projectNameString,
                runtime.status,
                "You cannot update the CPUs/Memory and the number of workers at the same time. We recommend you do this one at a time. The number of workers will be updated."
              )
            )
          else if (runtime.status == RuntimeStatus.Stopped)
            Async[F].pure((mt, false))
          else if (!allowStop)
            Async[F].raiseError[(MachineTypeName, Boolean)](RuntimeMachineTypeCannotBeChangedException(runtime))
          else Async[F].pure((mt, true))
      }
      // should master disk size be updated?
      targetMasterDiskSize <- traverseIfChanged(req.updatedMasterDiskSize, dataprocConfig.masterDiskSize) { d =>
        if (d.gb < dataprocConfig.masterDiskSize.gb)
          Async[F].raiseError[DiskSize](RuntimeDiskSizeCannotBeDecreasedException(runtime))
        else
          Async[F].pure(d)
      }
      // if any of the above is defined, send a PubSub message
      msg <- (targetNumWorkers orElse targetNumPreemptibles orElse targetMasterMachineType orElse targetMasterDiskSize)
        .traverse { _ =>
          val message = UpdateRuntimeMessage(
            runtime.id,
            targetMasterMachineType.map(_._1),
            targetMasterMachineType.map(_._2).getOrElse(false),
            targetMasterDiskSize,
            targetNumWorkers,
            targetNumPreemptibles,
            Some(traceId)
          )
          publisherQueue.enqueue1(message).as(message)
        }
    } yield msg
}

object RuntimeServiceInterp {
  private def convertToRuntime(userInfo: UserInfo,
                               serviceAccountInfo: WorkbenchEmail,
                               googleProject: GoogleProject,
                               runtimeName: RuntimeName,
                               clusterInternalId: RuntimeSamResource,
                               clusterImages: Set[RuntimeImage],
                               config: RuntimeServiceConfig,
                               req: CreateRuntime2Request,
                               now: Instant): Either[Throwable, Runtime] = {
    // create a LabelMap of default labels
    val defaultLabels = DefaultRuntimeLabels(
      runtimeName,
      googleProject,
      userInfo.userEmail,
      serviceAccountInfo,
      req.jupyterUserScriptUri,
      req.jupyterStartUserScriptUri,
      clusterImages.map(_.imageType).filterNot(_ == Welder).headOption
    ).toMap

    // combine default and given labels and add labels for extensions
    val allLabels = req.labels ++ defaultLabels ++ req.userJupyterExtensionConfig.map(_.asLabels).getOrElse(Map.empty)

    val autopauseThreshold = calculateAutopauseThreshold(
      req.autopause,
      req.autopauseThreshold.map(_.toMinutes.toInt),
      config.autoFreezeConfig
    ) //TODO: use FiniteDuration for autopauseThreshold field in Cluster
    val clusterScopes = req.runtimeConfig match {
      case Some(rq) if rq.cloudService == CloudService.GCE =>
        if (req.scopes.isEmpty) config.gceConfig.defaultScopes else req.scopes
      case Some(rq) if rq.cloudService == CloudService.Dataproc =>
        if (req.scopes.isEmpty) config.dataprocConfig.defaultScopes else req.scopes
      case None =>
        if (req.scopes.isEmpty) config.gceConfig.defaultScopes
        else req.scopes //default to create gce runtime if runtimeConfig is not specified
    }

    for {
      // check the labels do not contain forbidden keys
      labels <- if (allLabels.contains(includeDeletedKey))
        Left(IllegalLabelKeyException(includeDeletedKey))
      else
        Right(allLabels)
    } yield Runtime(
      0,
      samResource = clusterInternalId,
      runtimeName = runtimeName,
      googleProject = googleProject,
      serviceAccount = serviceAccountInfo,
      asyncRuntimeFields = None,
      auditInfo = AuditInfo(userInfo.userEmail, now, None, now),
      kernelFoundBusyDate = None,
      proxyUrl = Runtime.getProxyUrl(config.proxyUrlBase, googleProject, runtimeName, clusterImages, labels),
      status = RuntimeStatus.PreCreating,
      labels = labels,
      jupyterUserScriptUri = req.jupyterUserScriptUri,
      jupyterStartUserScriptUri = req.jupyterStartUserScriptUri,
      errors = List.empty,
      dataprocInstances = Set.empty,
      userJupyterExtensionConfig = req.userJupyterExtensionConfig,
      autopauseThreshold = autopauseThreshold,
      defaultClientId = req.defaultClientId,
      runtimeImages = clusterImages,
      scopes = clusterScopes,
      welderEnabled = true,
      customEnvironmentVariables = req.customEnvironmentVariables,
      allowStop = false, //TODO: double check this should be false when cluster is created
      runtimeConfigId = RuntimeConfigId(-1),
      stopAfterCreation = false,
      patchInProgress = false
    )
  }

  def traverseIfChanged[F[_]: Async, A, B](testVal: Option[A], existingVal: A)(fn: A => F[B]): F[Option[B]] =
    testVal.flatTraverse { x =>
      if (x != existingVal)
        fn(x).map(_.some)
      else
        Async[F].pure(none[B])
    }

  def processPersistentDiskRequest[F[_]](
    req: PersistentDiskRequest,
    googleProject: GoogleProject,
    userInfo: UserInfo,
    serviceAccount: WorkbenchEmail,
    willBeUsedBy: FormattedBy,
    authProvider: LeoAuthProvider[F],
    diskConfig: PersistentDiskConfig
  )(implicit as: ApplicativeAsk[F, AppContext],
    F: Async[F],
    dbReference: DbReference[F],
    ec: ExecutionContext,
    log: StructuredLogger[F]): F[PersistentDiskRequestResult] =
    for {
      ctx <- as.ask
      diskOpt <- persistentDiskQuery.getActiveByName(googleProject, req.name).transaction
      disk <- diskOpt match {
        case Some(pd) =>
          for {
            isAttached <- pd.formattedBy match {
              case None =>
                for {
                  isAttachedToRuntime <- RuntimeConfigQueries.isDiskAttached(pd.id).transaction
                  isAttached <- if (isAttachedToRuntime) F.pure(true)
                  else appQuery.isDiskAttached(pd.id).transaction
                } yield isAttached
              case Some(FormattedBy.Galaxy) =>
                if (willBeUsedBy == FormattedBy.Galaxy) //TODO: If we support more apps, we need to update this check
                  appQuery.isDiskAttached(pd.id).transaction
                else
                  F.raiseError[Boolean](
                    DiskAlreadyFormattedByOtherApp(googleProject, req.name, ctx.traceId, FormattedBy.Galaxy)
                  )
              case Some(FormattedBy.GCE) =>
                if (willBeUsedBy == FormattedBy.Galaxy)
                  F.raiseError[Boolean](
                    DiskAlreadyFormattedByOtherApp(googleProject, req.name, ctx.traceId, FormattedBy.GCE)
                  )
                else
                  RuntimeConfigQueries.isDiskAttached(pd.id).transaction
            }
            // throw 409 if the disk is attached to a runtime
            _ <- if (isAttached)
              F.raiseError[Unit](DiskAlreadyAttachedException(googleProject, req.name, ctx.traceId))
            else F.unit
            hasPermission <- authProvider.hasPersistentDiskPermission(pd.samResource,
                                                                      userInfo,
                                                                      PersistentDiskAction.AttachPersistentDisk,
                                                                      googleProject)
            _ <- if (hasPermission) F.unit else F.raiseError[Unit](AuthorizationError(Some(userInfo.userEmail)))
          } yield PersistentDiskRequestResult(pd, true)
        case None =>
          for {
            hasPermission <- authProvider.hasProjectPermission(userInfo, CreatePersistentDisk, googleProject)
            _ <- if (hasPermission) F.unit else F.raiseError[Unit](AuthorizationError(Some(userInfo.userEmail)))
            samResource <- F.delay(PersistentDiskSamResource(UUID.randomUUID().toString))
            diskBeforeSave <- F.fromEither(
              DiskServiceInterp.convertToDisk(userInfo,
                                              serviceAccount,
                                              googleProject,
                                              req.name,
                                              samResource,
                                              diskConfig,
                                              CreateDiskRequest.fromDiskConfigRequest(req),
                                              ctx.now)
            )
            _ <- authProvider
              .notifyResourceCreated(samResource, userInfo.userEmail, googleProject)
              .handleErrorWith { t =>
                log.error(t)(
                  s"[${ctx.traceId}] Failed to notify the AuthProvider for creation of persistent disk ${diskBeforeSave.projectNameString}"
                ) >> F.raiseError(t)
              }
            pd <- persistentDiskQuery.save(diskBeforeSave).transaction
          } yield PersistentDiskRequestResult(pd, false)
      }
    } yield disk

  case class PersistentDiskRequestResult(disk: PersistentDisk, doesExist: Boolean)
}

final case class RuntimeServiceConfig(proxyUrlBase: String,
                                      imageConfig: ImageConfig,
                                      autoFreezeConfig: AutoFreezeConfig,
                                      zombieRuntimeMonitorConfig: ZombieRuntimeMonitorConfig,
                                      dataprocConfig: DataprocConfig,
                                      gceConfig: GceConfig)

final case class WrongCloudServiceException(runtimeCloudService: CloudService,
                                            updateCloudService: CloudService,
                                            traceId: TraceId)
    extends LeoException(
      s"${traceId} | Bad request. This runtime is created with ${runtimeCloudService.asString}, and can not be updated to use ${updateCloudService.asString}",
      StatusCodes.Conflict
    )

final case class DiskNotSupportedException(traceId: TraceId)
    extends LeoException(
      s"${traceId} | Persistent disks are not supported on Google Cloud Dataproc",
      StatusCodes.Conflict
    )

final case class DiskAlreadyAttachedException(googleProject: GoogleProject, name: DiskName, traceId: TraceId)
    extends LeoException(
      s"${traceId} | Persistent disk ${googleProject.value}/${name.value} is already attached to another runtime",
      StatusCodes.Conflict
    )

final case class DiskAlreadyFormattedByOtherApp(googleProject: GoogleProject,
                                                name: DiskName,
                                                traceId: TraceId,
                                                formattedBy: FormattedBy)
    extends LeoException(
      s"${traceId} | Persistent disk ${googleProject.value}/${name.value} is already formatted by ${formattedBy.asString}",
      StatusCodes.Conflict
    )
