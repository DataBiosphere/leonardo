package org.broadinstitute.dsde.workbench.leonardo
package http
package service

import java.time.Instant
import java.util.UUID

import akka.http.scaladsl.model.StatusCodes
import cats.effect.concurrent.Semaphore
import cats.effect.{Async, Blocker, ContextShift, Timer}
import cats.implicits._
import cats.mtl.ApplicativeAsk
import cats.Parallel
import com.google.auth.oauth2.AccessToken
import com.google.cloud.BaseServiceException
import io.chrisdavenport.log4cats.StructuredLogger
import org.broadinstitute.dsde.workbench.google2.util.RetryPredicates
import org.broadinstitute.dsde.workbench.google2.{GcsBlobName, GoogleStorageService, MachineTypeName}
import org.broadinstitute.dsde.workbench.leonardo.RuntimeImageType.{Jupyter, Proxy, Welder}
import org.broadinstitute.dsde.workbench.leonardo.config.{AutoFreezeConfig, DataprocConfig, GceConfig, ImageConfig}
import org.broadinstitute.dsde.workbench.leonardo.dao.DockerDAO
import org.broadinstitute.dsde.workbench.leonardo.db.{
  clusterQuery,
  DbReference,
  LeonardoServiceDbQueries,
  RuntimeConfigQueries,
  SaveCluster
}
import org.broadinstitute.dsde.workbench.leonardo.http.api.{
  CreateRuntime2Request,
  UpdateRuntimeConfigRequest,
  UpdateRuntimeRequest
}
import org.broadinstitute.dsde.workbench.leonardo.http.service.LeonardoService._
import org.broadinstitute.dsde.workbench.leonardo.http.service.RuntimeServiceInterp._
import org.broadinstitute.dsde.workbench.leonardo.model._
import org.broadinstitute.dsde.workbench.leonardo.monitor.LeoPubsubMessage
import org.broadinstitute.dsde.workbench.leonardo.monitor.LeoPubsubMessage.{
  CreateRuntimeMessage,
  DeleteRuntimeMessage,
  StartRuntimeMessage,
  StopRuntimeMessage,
  UpdateRuntimeMessage
}
import org.broadinstitute.dsde.workbench.model.google.GoogleProject
import org.broadinstitute.dsde.workbench.model.{google, TraceId, UserInfo, WorkbenchEmail}

import scala.concurrent.ExecutionContext

class RuntimeServiceInterp[F[_]: Parallel](blocker: Blocker,
                                           semaphore: Semaphore[F],
                                           config: RuntimeServiceConfig,
                                           authProvider: LeoAuthProvider[F],
                                           serviceAccountProvider: ServiceAccountProvider[F],
                                           dockerDAO: DockerDAO[F],
                                           googleStorageService: GoogleStorageService[F],
                                           publisherQueue: fs2.concurrent.Queue[F, LeoPubsubMessage])(
  implicit F: Async[F],
  log: StructuredLogger[F],
  timer: Timer[F],
  cs: ContextShift[F],
  dbReference: DbReference[F],
  ec: ExecutionContext
) extends RuntimeService[F] {

  override def createRuntime(
    userInfo: UserInfo,
    googleProject: GoogleProject,
    runtimeName: RuntimeName,
    req: CreateRuntime2Request
  )(implicit as: ApplicativeAsk[F, AppContext]): F[Unit] =
    for {
      context <- as.ask
      hasPermission <- authProvider.hasProjectPermission(userInfo, ProjectActions.CreateClusters, googleProject)
      _ <- if (hasPermission) F.unit else F.raiseError[Unit](AuthorizationError(Some(userInfo.userEmail)))
      // Grab the service accounts from serviceAccountProvider for use later
      clusterServiceAccountOpt <- serviceAccountProvider
        .getClusterServiceAccount(userInfo, googleProject)
      notebookServiceAccountOpt <- serviceAccountProvider
        .getNotebookServiceAccount(userInfo, googleProject)
      serviceAccountInfo = ServiceAccountInfo(clusterServiceAccountOpt, notebookServiceAccountOpt)

      clusterOpt <- clusterQuery.getActiveClusterByNameMinimal(googleProject, runtimeName).transaction

      _ <- clusterOpt match {
        case Some(c) => F.raiseError[Unit](RuntimeAlreadyExistsException(googleProject, runtimeName, c.status))
        case None =>
          for {
            internalId <- F.delay(RuntimeInternalId(UUID.randomUUID().toString))
            petToken <- serviceAccountProvider.getAccessToken(userInfo.userEmail, googleProject).recoverWith {
              case e =>
                log.warn(e)(
                  s"Could not acquire pet service account access token for user ${userInfo.userEmail.value} in project $googleProject. " +
                    s"Skipping validation of bucket objects in the cluster request."
                ) as None
            }
            clusterImages <- getRuntimeImages(petToken,
                                              userInfo.userEmail,
                                              googleProject,
                                              context.now,
                                              req.toolDockerImage,
                                              req.welderDockerImage)
            runtime <- F.fromEither(
              convertToRuntime(userInfo,
                               serviceAccountInfo,
                               googleProject,
                               runtimeName,
                               internalId,
                               clusterImages,
                               config,
                               req,
                               context.now)
            )
            gcsObjectUrisToValidate = runtime.userJupyterExtensionConfig
              .map(
                config =>
                  (config.nbExtensions.values ++ config.serverExtensions.values ++ config.combinedExtensions.values)
                    .filter(_.startsWith("gs://"))
                    .toList
              )
              .getOrElse(List.empty) ++ req.jupyterUserScriptUri.map(_.asString) ++ req.jupyterStartUserScriptUri.map(
              _.asString
            )
            _ <- petToken.traverse(
              t =>
                gcsObjectUrisToValidate
                  .parTraverse(s => validateBucketObjectUri(userInfo.userEmail, t, s, context.traceId))
            )
            _ <- authProvider
              .notifyClusterCreated(internalId, userInfo.userEmail, googleProject, runtimeName)
              .handleErrorWith { t =>
                log.error(t)(
                  s"[${context.traceId}] Failed to notify the AuthProvider for creation of cluster ${runtime.projectNameString}"
                ) >> F.raiseError(t)
              }

            gceRuntimeDefaults = config.gceConfig.runtimeConfigDefaults
            runtimeCofig = req.runtimeConfig
              .fold[RuntimeConfig](gceRuntimeDefaults) { // default to gce if no runtime specific config is provided
                c =>
                  c match {
                    case gce: RuntimeConfigRequest.GceConfig =>
                      RuntimeConfig.GceConfig(
                        gce.machineType.getOrElse(gceRuntimeDefaults.machineType),
                        gce.diskSize.getOrElse(gceRuntimeDefaults.diskSize)
                      ): RuntimeConfig
                    case dataproc: RuntimeConfigRequest.DataprocConfig =>
                      dataproc.toRuntimeConfigDataprocConfig(config.dataprocConfig.runtimeConfigDefaults): RuntimeConfig
                  }
              }

            saveCluster = SaveCluster(cluster = runtime, runtimeConfig = runtimeCofig, now = context.now)
            runtime <- clusterQuery.save(saveCluster).transaction
            _ <- publisherQueue.enqueue1(CreateRuntimeMessage.fromRuntime(runtime, runtimeCofig, Some(context.traceId)))
          } yield ()
      }
    } yield ()

  override def getRuntime(userInfo: UserInfo, googleProject: GoogleProject, runtimeName: RuntimeName)(
    implicit as: ApplicativeAsk[F, AppContext]
  ): F[GetRuntimeResponse] =
    for {
      // throws 404 if not existent
      resp <- LeonardoServiceDbQueries.getGetClusterResponse(googleProject, runtimeName).transaction
      // throw 404 if no GetClusterStatus permission
      hasPermission <- authProvider.hasNotebookClusterPermission(resp.internalId,
                                                                 userInfo,
                                                                 NotebookClusterActions.GetClusterStatus,
                                                                 googleProject,
                                                                 runtimeName)
      _ <- if (hasPermission) F.unit else F.raiseError[Unit](RuntimeNotFoundException(googleProject, runtimeName))
    } yield resp

  override def listRuntimes(userInfo: UserInfo, googleProject: Option[GoogleProject], params: Map[String, String])(
    implicit as: ApplicativeAsk[F, AppContext]
  ): F[Vector[ListRuntimeResponse]] =
    for {
      paramMap <- F.fromEither(processListClustersParameters(params))
      clusters <- LeonardoServiceDbQueries.listClusters(paramMap._1, paramMap._2, googleProject).transaction
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

  override def deleteRuntime(userInfo: UserInfo, googleProject: GoogleProject, runtimeName: RuntimeName)(
    implicit as: ApplicativeAsk[F, AppContext]
  ): F[Unit] =
    for {
      // throw 404 if not existent
      runtimeOpt <- clusterQuery.getActiveClusterByNameMinimal(googleProject, runtimeName).transaction
      runtime <- runtimeOpt.fold(F.raiseError[Runtime](RuntimeNotFoundException(googleProject, runtimeName)))(F.pure)
      // throw 404 if no GetClusterStatus permission
      // Note: the general pattern is to 404 (e.g. pretend the runtime doesn't exist) if the caller doesn't have
      // GetClusterStatus permission. We return 403 if the user can view the runtime but can't perform some other action.
      hasPermission <- authProvider.hasNotebookClusterPermission(runtime.internalId,
                                                                 userInfo,
                                                                 NotebookClusterActions.GetClusterStatus,
                                                                 googleProject,
                                                                 runtimeName)
      _ <- if (hasPermission) F.unit else F.raiseError[Unit](RuntimeNotFoundException(googleProject, runtimeName))
      // throw 403 if no DeleteCluster permission
      hasDeletePermission <- authProvider.hasNotebookClusterPermission(runtime.internalId,
                                                                       userInfo,
                                                                       NotebookClusterActions.DeleteCluster,
                                                                       googleProject,
                                                                       runtimeName)
      _ <- if (hasDeletePermission) F.unit else F.raiseError[Unit](AuthorizationError(Some(userInfo.userEmail)))
      // throw 409 if the cluster is not deletable
      _ <- if (runtime.status.isDeletable) F.unit
      else F.raiseError[Unit](RuntimeCannotBeDeletedException(runtime.googleProject, runtime.runtimeName))
      // delete the runtime
      ctx <- as.ask
      _ <- if (runtime.asyncRuntimeFields.isDefined) {
        clusterQuery.markPendingDeletion(runtime.id, ctx.now).transaction.void >> publisherQueue.enqueue1(
          DeleteRuntimeMessage(runtime.id, Some(ctx.traceId))
        )
      } else {
        clusterQuery.completeDeletion(runtime.id, ctx.now).transaction.void >> authProvider.notifyClusterDeleted(
          runtime.internalId,
          runtime.auditInfo.creator,
          runtime.auditInfo.creator,
          runtime.googleProject,
          runtime.runtimeName
        )
      }
    } yield ()

  def stopRuntime(userInfo: UserInfo, googleProject: GoogleProject, runtimeName: RuntimeName)(
    implicit as: ApplicativeAsk[F, AppContext]
  ): F[Unit] =
    for {
      // throw 404 if not existent
      runtimeOpt <- clusterQuery.getActiveClusterByNameMinimal(googleProject, runtimeName).transaction
      runtime <- runtimeOpt.fold(F.raiseError[Runtime](RuntimeNotFoundException(googleProject, runtimeName)))(F.pure)
      // throw 404 if no GetClusterStatus permission
      // Note: the general pattern is to 404 (e.g. pretend the runtime doesn't exist) if the caller doesn't have
      // GetClusterStatus permission. We return 403 if the user can view the runtime but can't perform some other action.
      hasPermission <- authProvider.hasNotebookClusterPermission(runtime.internalId,
                                                                 userInfo,
                                                                 NotebookClusterActions.GetClusterStatus,
                                                                 googleProject,
                                                                 runtimeName)
      _ <- if (hasPermission) F.unit else F.raiseError[Unit](RuntimeNotFoundException(googleProject, runtimeName))
      // throw 403 if no StopStartCluster permission
      hasStopPermission <- authProvider.hasNotebookClusterPermission(runtime.internalId,
                                                                     userInfo,
                                                                     NotebookClusterActions.StopStartCluster,
                                                                     googleProject,
                                                                     runtimeName)
      _ <- if (hasStopPermission) F.unit else F.raiseError[Unit](AuthorizationError(Some(userInfo.userEmail)))
      // throw 409 if the cluster is not stoppable
      _ <- if (runtime.status.isStoppable) F.unit
      else
        F.raiseError[Unit](RuntimeCannotBeStoppedException(runtime.googleProject, runtime.runtimeName, runtime.status))
      // stop the runtime
      ctx <- as.ask
      _ <- clusterQuery.setToStopping(runtime.id, ctx.now).transaction
      _ <- publisherQueue.enqueue1(StopRuntimeMessage(runtime.id, Some(ctx.traceId)))
    } yield ()

  def startRuntime(userInfo: UserInfo, googleProject: GoogleProject, runtimeName: RuntimeName)(
    implicit as: ApplicativeAsk[F, AppContext]
  ): F[Unit] =
    for {
      // throw 404 if not existent
      runtimeOpt <- clusterQuery.getActiveClusterByNameMinimal(googleProject, runtimeName).transaction
      runtime <- runtimeOpt.fold(F.raiseError[Runtime](RuntimeNotFoundException(googleProject, runtimeName)))(F.pure)
      // throw 404 if no GetClusterStatus permission
      // Note: the general pattern is to 404 (e.g. pretend the runtime doesn't exist) if the caller doesn't have
      // GetClusterStatus permission. We return 403 if the user can view the runtime but can't perform some other action.
      hasPermission <- authProvider.hasNotebookClusterPermission(runtime.internalId,
                                                                 userInfo,
                                                                 NotebookClusterActions.GetClusterStatus,
                                                                 googleProject,
                                                                 runtimeName)
      _ <- if (hasPermission) F.unit else F.raiseError[Unit](RuntimeNotFoundException(googleProject, runtimeName))
      // throw 403 if no StopStartCluster permission
      hasStartPermission <- authProvider.hasNotebookClusterPermission(runtime.internalId,
                                                                      userInfo,
                                                                      NotebookClusterActions.StopStartCluster,
                                                                      googleProject,
                                                                      runtimeName)
      _ <- if (hasStartPermission) F.unit else F.raiseError[Unit](AuthorizationError(Some(userInfo.userEmail)))
      // throw 409 if the cluster is not startable
      _ <- if (runtime.status.isStartable) F.unit
      else
        F.raiseError[Unit](RuntimeCannotBeStartedException(runtime.googleProject, runtime.runtimeName, runtime.status))
      // start the runtime
      ctx <- as.ask
      _ <- clusterQuery.updateClusterStatus(runtime.id, RuntimeStatus.Starting, ctx.now).transaction
      _ <- publisherQueue.enqueue1(StartRuntimeMessage(runtime.id, Some(ctx.traceId)))
    } yield ()

  override def updateRuntime(
    userInfo: UserInfo,
    googleProject: GoogleProject,
    runtimeName: RuntimeName,
    req: UpdateRuntimeRequest
  )(implicit as: ApplicativeAsk[F, AppContext]): F[Unit] =
    for {
      // throw 404 if not existent
      runtimeOpt <- clusterQuery.getActiveClusterByNameMinimal(googleProject, runtimeName).transaction
      runtime <- runtimeOpt.fold(F.raiseError[Runtime](RuntimeNotFoundException(googleProject, runtimeName)))(F.pure)
      // throw 404 if no GetClusterStatus permission
      // Note: the general pattern is to 404 (e.g. pretend the runtime doesn't exist) if the caller doesn't have
      // GetClusterStatus permission. We return 403 if the user can view the runtime but can't perform some other action.
      hasPermission <- authProvider.hasNotebookClusterPermission(runtime.internalId,
                                                                 userInfo,
                                                                 NotebookClusterActions.GetClusterStatus,
                                                                 googleProject,
                                                                 runtimeName)
      _ <- if (hasPermission) F.unit else F.raiseError[Unit](RuntimeNotFoundException(googleProject, runtimeName))
      // throw 403 if no ModifyCluster permission
      hasModifyPermission <- authProvider.hasNotebookClusterPermission(runtime.internalId,
                                                                       userInfo,
                                                                       NotebookClusterActions.ModifyCluster,
                                                                       googleProject,
                                                                       runtimeName)
      _ <- if (hasModifyPermission) F.unit else F.raiseError[Unit](AuthorizationError(Some(userInfo.userEmail)))
      // throw 409 if the cluster is not updatable
      _ <- if (runtime.status.isUpdatable) F.unit
      else
        F.raiseError[Unit](RuntimeCannotBeUpdatedException(runtime.projectNameString, runtime.status))
      runtimeConfig <- RuntimeConfigQueries.getRuntimeConfig(runtime.runtimeConfigId).transaction
      ctx <- as.ask
      // Updating autopause is just a DB update, so we can do it here instead of sending a PubSub message
      updatedAutopauseThreshold = calculateAutopauseThreshold(req.updateAutopauseEnabled,
                                                              req.updateAutopauseThreshold.map(_.toMinutes.toInt),
                                                              config.autoFreezeConfig)
      _ <- if (updatedAutopauseThreshold != runtime.autopauseThreshold)
        clusterQuery.updateAutopauseThreshold(runtime.id, updatedAutopauseThreshold, ctx.now).transaction.void
      else Async[F].unit
      // Updating the runtime config will potentially generate a PubSub message
      _ <- req.updatedRuntimeConfig.traverse_(
        update => processUpdateRuntimeConfigRequest(update, req.allowStop, runtime, runtimeConfig, ctx.traceId)
      )
    } yield ()

  private[service] def getRuntimeImages(
    petToken: Option[String],
    userEmail: WorkbenchEmail,
    googleProject: GoogleProject,
    now: Instant,
    toolDockerImage: Option[ContainerImage],
    welderDockerImage: Option[ContainerImage]
  )(implicit ev: ApplicativeAsk[F, TraceId]): F[Set[RuntimeImage]] =
    for {
      traceId <- ev.ask
      // Try to autodetect the image
      autodetectedImageOpt <- toolDockerImage.traverse { image =>
        dockerDAO.detectTool(image, petToken).flatMap {
          case None       => F.raiseError[RuntimeImage](ImageNotFoundException(traceId, image))
          case Some(tool) => F.pure(RuntimeImage(tool, image.imageUrl, now))
        }
      }
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
        val retryPolicy = RetryPredicates.retryConfigWithPredicates(
          RetryPredicates.standardRetryPredicate,
          RetryPredicates.whenStatusCode(401)
        )

        val res = for {
          blob <- GoogleStorageService.fromAccessToken(accessToken, blocker, Some(semaphore)).use { gcs =>
            gcs
              .getBlob(gcsPath.bucketName, GcsBlobName(gcsPath.objectName.value), Some(traceId), retryPolicy)
              .compile
              .last
          }
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
    (runtimeConfig, request) match {
      case (gceConfig @ RuntimeConfig.GceConfig(_, _), req @ UpdateRuntimeConfigRequest.GceConfig(_, _)) =>
        processUpdateGceConfigRequest(req, allowStop, runtime, gceConfig, traceId)

      case (dataprocConfig @ RuntimeConfig.DataprocConfig(_, _, _, _, _, _, _, _),
            req @ UpdateRuntimeConfigRequest.DataprocConfig(_, _, _, _)) =>
        processUpdateDataprocConfigRequest(req, allowStop, runtime, dataprocConfig, traceId)

      case _ =>
        Async[F].raiseError(WrongCloudServiceException(runtimeConfig.cloudService, request.cloudService))
    }

  private[service] def processUpdateGceConfigRequest(req: UpdateRuntimeConfigRequest.GceConfig,
                                                     allowStop: Boolean,
                                                     runtime: Runtime,
                                                     gceConfig: RuntimeConfig.GceConfig,
                                                     traceId: TraceId): F[Unit] =
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
      _ <- (targetMachineType orElse targetDiskSize).traverse_ { _ =>
        val message = UpdateRuntimeMessage(runtime.id,
                                           targetMachineType.map(_._1),
                                           targetMachineType.map(_._2).getOrElse(false),
                                           targetDiskSize,
                                           None,
                                           None,
                                           Some(traceId))
        publisherQueue.enqueue1(message)
      }
    } yield ()

  private[service] def processUpdateDataprocConfigRequest(req: UpdateRuntimeConfigRequest.DataprocConfig,
                                                          allowStop: Boolean,
                                                          runtime: Runtime,
                                                          dataprocConfig: RuntimeConfig.DataprocConfig,
                                                          traceId: TraceId): F[Unit] =
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
      _ <- (targetNumWorkers orElse targetNumPreemptibles orElse targetMasterMachineType orElse targetMasterDiskSize)
        .traverse_ { _ =>
          val message = UpdateRuntimeMessage(
            runtime.id,
            targetMasterMachineType.map(_._1),
            targetMasterMachineType.map(_._2).getOrElse(false),
            targetMasterDiskSize,
            targetNumWorkers,
            targetNumPreemptibles,
            Some(traceId)
          )
          publisherQueue.enqueue1(message)
        }
    } yield ()

}

object RuntimeServiceInterp {
  private def convertToRuntime(userInfo: UserInfo,
                               serviceAccountInfo: ServiceAccountInfo,
                               googleProject: GoogleProject,
                               runtimeName: RuntimeName,
                               clusterInternalId: RuntimeInternalId,
                               clusterImages: Set[RuntimeImage],
                               config: RuntimeServiceConfig,
                               req: CreateRuntime2Request,
                               now: Instant): Either[Throwable, Runtime] = {
    // create a LabelMap of default labels
    val defaultLabels = DefaultLabels(
      runtimeName,
      googleProject,
      userInfo.userEmail,
      serviceAccountInfo.clusterServiceAccount,
      serviceAccountInfo.notebookServiceAccount,
      req.jupyterUserScriptUri,
      req.jupyterStartUserScriptUri,
      clusterImages.map(_.imageType).filterNot(_ == Welder).headOption
    ).toMap

    val userJupyterExt = req.jupyterExtensionUri match {
      case Some(ext) => Map("notebookExtension" -> ext.toUri)
      case None      => Map.empty[String, String]
    }

    // add the userJupyterExt to the nbExtensions
    val updatedUserJupyterExtensionConfig = req.userJupyterExtensionConfig match {
      case Some(config) => config.copy(nbExtensions = config.nbExtensions ++ userJupyterExt)
      case None         => UserJupyterExtensionConfig(userJupyterExt, Map.empty, Map.empty, Map.empty)
    }

    // combine default and given labels and add labels for extensions
    val allLabels = req.labels ++ defaultLabels ++ updatedUserJupyterExtensionConfig.asLabels

    val autopauseThreshold = calculateAutopauseThreshold(
      req.autopause,
      req.autopauseThreshold.map(_.toMinutes.toInt),
      config.autoFreezeConfig
    ) //TODO: use FiniteDuration for autopauseThreshold field in Cluster
    val clusterScopes = if (req.scopes.isEmpty) config.dataprocConfig.defaultScopes else req.scopes //TODO: Rob will update this to GCE specific scopes appropriately
    for {
      // check the labels do not contain forbidden keys
      labels <- if (allLabels.contains(includeDeletedKey))
        Left(IllegalLabelKeyException(includeDeletedKey))
      else
        Right(allLabels)
    } yield Runtime(
      0,
      internalId = clusterInternalId,
      runtimeName = runtimeName,
      googleProject = googleProject,
      serviceAccountInfo = serviceAccountInfo,
      asyncRuntimeFields = None,
      auditInfo = AuditInfo(userInfo.userEmail, now, None, now, None),
      proxyUrl = Runtime.getProxyUrl(config.proxyUrlBase, googleProject, runtimeName, clusterImages, labels),
      status = RuntimeStatus.Creating,
      labels = labels,
      jupyterExtensionUri = req.jupyterExtensionUri,
      jupyterUserScriptUri = req.jupyterUserScriptUri,
      jupyterStartUserScriptUri = req.jupyterStartUserScriptUri,
      errors = List.empty,
      dataprocInstances = Set.empty,
      userJupyterExtensionConfig =
        if (updatedUserJupyterExtensionConfig.asLabels.isEmpty) None else Some(updatedUserJupyterExtensionConfig),
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
}

final case class RuntimeServiceConfig(proxyUrlBase: String,
                                      imageConfig: ImageConfig,
                                      autoFreezeConfig: AutoFreezeConfig,
                                      dataprocConfig: DataprocConfig,
                                      gceConfig: GceConfig)

final case class WrongCloudServiceException(runtimeCloudService: CloudService, updateCloudService: CloudService)
    extends LeoException(
      s"Bad request. This runtime is created with ${runtimeCloudService.asString}, and can not be updated to use ${updateCloudService.asString}",
      StatusCodes.Conflict
    )
