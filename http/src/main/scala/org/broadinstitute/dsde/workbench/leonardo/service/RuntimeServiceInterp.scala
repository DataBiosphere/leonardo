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
import org.broadinstitute.dsde.workbench.google2.{GcsBlobName, GoogleStorageService}
import org.broadinstitute.dsde.workbench.leonardo.RuntimeImageType.{Jupyter, Proxy, Welder}
import org.broadinstitute.dsde.workbench.leonardo.config.{AutoFreezeConfig, DataprocConfig, GceConfig, ImageConfig}
import org.broadinstitute.dsde.workbench.leonardo.dao.DockerDAO
import org.broadinstitute.dsde.workbench.leonardo.db.{DbReference, LeonardoServiceDbQueries, RuntimeConfigQueries, SaveCluster, clusterQuery}
import org.broadinstitute.dsde.workbench.leonardo.http.api.{CreateRuntime2Request, RuntimeServiceContext, UpdateRuntimeConfigRequest, UpdateRuntimeRequest}
import org.broadinstitute.dsde.workbench.leonardo.http.service.LeonardoService._
import org.broadinstitute.dsde.workbench.leonardo.http.service.RuntimeServiceInterp._
import org.broadinstitute.dsde.workbench.leonardo.model._
import org.broadinstitute.dsde.workbench.leonardo.monitor.LeoPubsubMessage
import org.broadinstitute.dsde.workbench.leonardo.monitor.LeoPubsubMessage.{CreateRuntimeMessage, DeleteRuntimeMessage, StartRuntimeMessage, StopRuntimeMessage, UpdateRuntimeMessage}
import org.broadinstitute.dsde.workbench.model.google.GoogleProject
import org.broadinstitute.dsde.workbench.model.{TraceId, UserInfo, WorkbenchEmail, google}

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
  )(implicit as: ApplicativeAsk[F, RuntimeServiceContext]): F[Unit] =
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
    implicit as: ApplicativeAsk[F, RuntimeServiceContext]
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
    implicit as: ApplicativeAsk[F, RuntimeServiceContext]
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
    implicit as: ApplicativeAsk[F, RuntimeServiceContext]
  ): F[Unit] =
    for {
      // throw 404 if not existent
      runtimeOpt <- clusterQuery.getActiveClusterByNameMinimal(googleProject, runtimeName).transaction
      runtime <- runtimeOpt.fold(F.raiseError[Runtime](RuntimeNotFoundException(googleProject, runtimeName)))(F.pure)
      // throw 404 if no GetClusterStatus permission
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
    implicit as: ApplicativeAsk[F, RuntimeServiceContext]
  ): F[Unit] =
    for {
      // throw 404 if not existent
      runtimeOpt <- clusterQuery.getActiveClusterByNameMinimal(googleProject, runtimeName).transaction
      runtime <- runtimeOpt.fold(F.raiseError[Runtime](RuntimeNotFoundException(googleProject, runtimeName)))(F.pure)
      // throw 404 if no GetClusterStatus permission
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
    implicit as: ApplicativeAsk[F, RuntimeServiceContext]
  ): F[Unit] =
    for {
      // throw 404 if not existent
      runtimeOpt <- clusterQuery.getActiveClusterByNameMinimal(googleProject, runtimeName).transaction
      runtime <- runtimeOpt.fold(F.raiseError[Runtime](RuntimeNotFoundException(googleProject, runtimeName)))(F.pure)
      // throw 404 if no GetClusterStatus permission
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

  override def updateRuntime(userInfo: UserInfo, googleProject: GoogleProject, runtimeName: RuntimeName, req: UpdateRuntimeRequest)(implicit as: ApplicativeAsk[F, RuntimeServiceContext]): F[Unit] = {
    for {
      // throw 404 if not existent
      runtimeOpt <- clusterQuery.getActiveClusterByNameMinimal(googleProject, runtimeName).transaction
      runtime <- runtimeOpt.fold(F.raiseError[Runtime](RuntimeNotFoundException(googleProject, runtimeName)))(F.pure)
      // throw 404 if no GetClusterStatus permission
      hasPermission <- authProvider.hasNotebookClusterPermission(runtime.internalId,
        userInfo,
        NotebookClusterActions.GetClusterStatus,
        googleProject,
        runtimeName)
      _ <- if (hasPermission) F.unit else F.raiseError[Unit](RuntimeNotFoundException(googleProject, runtimeName))
      // throw 403 if no ModifyCluster permission
      hasStartPermission <- authProvider.hasNotebookClusterPermission(runtime.internalId,
        userInfo,
        NotebookClusterActions.ModifyCluster,
        googleProject,
        runtimeName)
      _ <- if (hasStartPermission) F.unit else F.raiseError[Unit](AuthorizationError(Some(userInfo.userEmail)))
      runtimeConfig <- RuntimeConfigQueries.getRuntimeConfig(runtime.runtimeConfigId).transaction
      ctx <- as.ask

      updatedAutopauseThreshold = calculateAutopauseThreshold(req.updateAutopauseEnabled, req.updateAutopauseThreshold.map(_.toMinutes.toInt), config.autoFreezeConfig)
      _ <- if (updatedAutopauseThreshold != runtime.autopauseThreshold)
        clusterQuery.updateAutopauseThreshold(runtime.id, updatedAutopauseThreshold, ctx.now).transaction.void
      else Async[F].unit

      merged <- req.updatedRuntimeConfig.flatTraverse { updatedConfig =>
        mergeUpdateRuntimeConfig(updatedConfig, runtimeConfig)
      }
      _ <- merged.traverse { r =>
        publisherQueue.enqueue1(UpdateRuntimeMessage(runtime.id, r, Some(ctx.traceId)))
      }
    } yield ()
  }

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
              s"User ${userEmail.value} does not have access to ${gcsPath.bucketName} / ${gcsPath.objectName}"
            ) >> F.raiseError(BucketObjectAccessException(userEmail, gcsPath))
          case e: BaseServiceException if e.getCode == 401 =>
            log.warn(e)(s"Could not validate object [${gcsUri}] as user [${userEmail.value}]")
        }
    }
  }

  private[service] def mergeUpdateRuntimeConfig(req: UpdateRuntimeConfigRequest, runtimeConfig: RuntimeConfig): F[Option[RuntimeConfig]] = {
    Async[F].pure(None)

    //    _ <- req.updatedRuntimeConfig.traverse_ {
    //      case UpdateRuntimeConfigRequest.GceConfig(updatedMachineType, updatedDiskSize) =>
    //        for {
    //          _ <- if (runtimeConfig.cloudService != CloudService.GCE) Async[F].raiseError[Unit](WrongCloudServiceException(runtimeConfig.cloudService, CloudService.GCE)) else Async[F].unit
    //
    //          _ <- updatedMachineType.traverse_ {
    //            case mt if mt != runtimeConfig.machineType =>
    //              if (runtime.status == RuntimeStatus.Stopped || req.allowStop.getOrElse(true))
    //                publisherQueue.enqueue1(UpdateMachineTypeMessage(runtime.id, mt, Some(ctx.traceId)))
    //              else
    //                Async[F].raiseError[Unit](RuntimeMachineTypeCannotBeChangedException(runtime))
    //            case _ => Async[F].unit
    //          }
    //          _ <- updatedDiskSize.traverse_ {
    //            case d if d < runtimeConfig.diskSize =>
    //              Async[F].raiseError[Unit](RuntimeDiskSizeCannotBeDecreasedException(runtime))
    //            case d if d > runtimeConfig.diskSize =>
    //              publisherQueue.enqueue1(UpdateDiskSizeMessage(runtime.id, d, Some(ctx.traceId)))
    //            case _ => Async[F].unit
    //          }
    //        } yield ()
    //
    //      case UpdateRuntimeConfigRequest.DataprocConfig(updatedMasterMachineType, updatedMasterDiskSize, updatedNumberOfWorkers, updatedNumberOfPreemptibleWorkers) =>
    //        for {
    //          _ <- if (runtimeConfig.cloudService != CloudService.Dataproc) Async[F].raiseError[Unit](WrongCloudServiceException(runtimeConfig.cloudService, CloudService.Dataproc)) else Async[F].unit
    //          dataprocConfig = runtimeConfig.asInstanceOf[RuntimeConfig.DataprocConfig]
    //          _ <- Ior.fromOptions(updatedNumberOfWorkers, updatedNumberOfPreemptibleWorkers).traverse_ { x =>
    //            if (runtime.status == RuntimeStatus.Stopped)
    //              Async[F].raiseError[Unit](RuntimeCannotBeUpdatedException(
    //                runtime.projectNameString,
    //                runtime.status,
    //                "You cannot update the number of workers in a stopped runtime. Please start your runtime to perform this action."
    //              ))
    //            else
    //              x match {
    //                case Ior.Left(w) if w != dataprocConfig.numberOfWorkers =>
    //                  publisherQueue.enqueue1(UpdateDataprocWorkersMessage(runtime.id, Some(w), None, Some(ctx.traceId)))
    //                case Ior.Right(p) if p != dataprocConfig.numberOfPreemptibleWorkers.getOrElse(0) =>
    //                  publisherQueue.enqueue1(UpdateDataprocWorkersMessage(runtime.id, None, Some(p), Some(ctx.traceId)))
    //                case Ior.Both(w, p) if w != dataprocConfig.numberOfWorkers && p != dataprocConfig.numberOfPreemptibleWorkers.getOrElse(0) =>
    //                  publisherQueue.enqueue1(UpdateDataprocWorkersMessage(runtime.id, Some(w), Some(p), Some(ctx.traceId)))
    //                case Ior.Both(w, p) if w != dataprocConfig.numberOfWorkers =>
    //
    //              }
    //          }
    //        } yield ()
    //
    //    }
  }

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
      stopAfterCreation = false
    )
  }
}

final case class RuntimeServiceConfig(proxyUrlBase: String,
                                      imageConfig: ImageConfig,
                                      autoFreezeConfig: AutoFreezeConfig,
                                      dataprocConfig: DataprocConfig,
                                      gceConfig: GceConfig)

//final case class WrongCloudServiceException(runtimeCloudService: CloudService, updateCloudService: CloudService)
//  extends LeoException(s"Bad request. This runtime is created with ${runtimeCloudService.asString}, and can not be updated to use ${updateCloudService.asString}", StatusCodes.409)