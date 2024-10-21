package org.broadinstitute.dsde.workbench.leonardo
package http
package service

import akka.http.scaladsl.model.StatusCodes
import cats.Parallel
import cats.effect.Async
import cats.effect.std.Queue
import cats.mtl.Ask
import cats.syntax.all._
import com.google.auth.oauth2.{AccessToken, GoogleCredentials}
import com.google.cloud.BaseServiceException
import org.broadinstitute.dsde.workbench.leonardo.db.DBIOInstances._
import org.broadinstitute.dsde.workbench.google2.util.RetryPredicates
import org.broadinstitute.dsde.workbench.google2.{
  DiskName,
  GcsBlobName,
  GoogleComputeService,
  GoogleStorageService,
  MachineTypeName,
  ZoneName
}
import org.broadinstitute.dsde.workbench.leonardo.JsonCodec._
import org.broadinstitute.dsde.workbench.leonardo.RuntimeImageType.{CryptoDetector, Jupyter, Proxy, Welder}
import org.broadinstitute.dsde.workbench.leonardo.SamResourceId.{
  PersistentDiskSamResourceId,
  ProjectSamResourceId,
  RuntimeSamResourceId,
  WorkspaceResourceSamResourceId
}
import org.broadinstitute.dsde.workbench.leonardo.config._
import org.broadinstitute.dsde.workbench.leonardo.dao.DockerDAO
import org.broadinstitute.dsde.workbench.leonardo.dao.sam.SamService
import org.broadinstitute.dsde.workbench.leonardo.db._
import org.broadinstitute.dsde.workbench.leonardo.http.service.DiskServiceInterp.getDiskSamPolicyMap
import org.broadinstitute.dsde.workbench.leonardo.model.SamResourceAction.{
  projectSamResourceAction,
  runtimeSamResourceAction,
  workspaceSamResourceAction
}
import org.broadinstitute.dsde.workbench.leonardo.http.service.RuntimeServiceInterp._
import org.broadinstitute.dsde.workbench.leonardo.model.SamResourceAction._
import org.broadinstitute.dsde.workbench.leonardo.model._
import org.broadinstitute.dsde.workbench.leonardo.monitor.LeoPubsubMessage._
import org.broadinstitute.dsde.workbench.leonardo.monitor.{
  DiskUpdate,
  LeoPubsubMessage,
  RuntimeConfigInCreateRuntimeMessage,
  RuntimePatchDetails
}
import org.broadinstitute.dsde.workbench.model.google.GoogleProject
import org.broadinstitute.dsde.workbench.model.{google, TraceId, UserInfo, WorkbenchEmail}
import org.broadinstitute.dsde.workbench.openTelemetry.OpenTelemetryMetrics
import org.broadinstitute.dsde.workbench.util2.InstanceName
import org.typelevel.log4cats.StructuredLogger
import slick.dbio.DBIOAction

import java.time.Instant
import java.util.UUID
import scala.concurrent.ExecutionContext

class RuntimeServiceInterp[F[_]: Parallel](
  config: RuntimeServiceConfig,
  diskConfig: PersistentDiskConfig,
  authProvider: LeoAuthProvider[F],
  dockerDAO: DockerDAO[F],
  googleStorageService: Option[GoogleStorageService[F]],
  googleComputeService: Option[GoogleComputeService[F]],
  publisherQueue: Queue[F, LeoPubsubMessage],
  samService: SamService[F]
)(implicit
  F: Async[F],
  log: StructuredLogger[F],
  dbReference: DbReference[F],
  ec: ExecutionContext,
  metrics: OpenTelemetryMetrics[F]
) extends RuntimeService[F] {

  override def createRuntime(
    userInfo: UserInfo,
    cloudContext: CloudContext,
    runtimeName: RuntimeName,
    req: CreateRuntimeRequest
  )(implicit as: Ask[F, AppContext]): F[CreateRuntimeResponse] =
    for {
      context <- as.ask
      googleProject <- F.fromOption(
        LeoLenses.cloudContextToGoogleProject.get(cloudContext),
        AzureUnimplementedException("Azure runtime is not supported yet")
      )
      // Resolve the user email in Sam from the user token. This translates a pet token to the owner email.
      userEmail <- samService.getUserEmail(userInfo.accessToken.token)
      hasPermission <- authProvider.hasPermission[ProjectSamResourceId, ProjectAction](
        ProjectSamResourceId(googleProject),
        ProjectAction.CreateRuntime,
        userInfo
      )
      _ <- context.span.traverse(s => F.delay(s.addAnnotation("Done Sam call for cluster permission")))
      _ <- F.raiseUnless(hasPermission)(ForbiddenError(userEmail))
      // Grab the pet service account for the user
      petSA <- samService.getPetServiceAccount(userInfo.accessToken.token, googleProject)
      _ <- context.span.traverse(s => F.delay(s.addAnnotation("Done Sam call for getPetServiceAccount")))

      // Retrieve parent workspaceId for the google project
      parentWorkspaceId <- samService.lookupWorkspaceParentForGoogleProject(userInfo.accessToken.token, googleProject)

      runtimeOpt <- RuntimeServiceDbQueries.getStatusByName(cloudContext, runtimeName).transaction
      _ <- context.span.traverse(s => F.delay(s.addAnnotation("Done DB query for active cluster")))
      _ <- runtimeOpt match {
        case Some(status) => F.raiseError[Unit](RuntimeAlreadyExistsException(cloudContext, runtimeName, status))
        case None =>
          for {
            samResource <- F.delay(RuntimeSamResourceId(UUID.randomUUID().toString))
            // Get a GCP pet service account token to resolve GCP objects like bucket objects, GCR images.
            // We can't use the user token directly because it is a B2C token.
            petToken <- samService.getPetServiceAccountToken(userEmail, googleProject)
            runtimeImages <- getRuntimeImages(Some(petToken), context.now, req.toolDockerImage, req.welderRegistry)
            _ <- context.span.traverse(s => F.delay(s.addAnnotation("Done get runtime images")))
            // .get here should be okay since this is from config, and it should always be defined; Ideally we probaly should use a different type for reading this config than RuntimeConfig
            bootDiskSize = config.gceConfig.runtimeConfigDefaults.bootDiskSize.get

            defaultRuntimeConfig = RuntimeConfigInCreateRuntimeMessage.GceConfig(
              config.gceConfig.runtimeConfigDefaults.machineType,
              config.gceConfig.runtimeConfigDefaults.diskSize,
              bootDiskSize,
              config.gceConfig.runtimeConfigDefaults.zone,
              None
            )

            runtimeConfig <- req.runtimeConfig
              .fold[F[RuntimeConfigInCreateRuntimeMessage]](F.pure(defaultRuntimeConfig)) { // default to gce if no runtime specific config is provided
                c =>
                  c match {
                    case gce: RuntimeConfigRequest.GceConfig =>
                      F.pure(
                        RuntimeConfigInCreateRuntimeMessage.GceConfig(
                          gce.machineType.getOrElse(config.gceConfig.runtimeConfigDefaults.machineType),
                          gce.diskSize.getOrElse(config.gceConfig.runtimeConfigDefaults.diskSize),
                          bootDiskSize,
                          gce.zone.getOrElse(config.gceConfig.runtimeConfigDefaults.zone),
                          gce.gpuConfig
                        ): RuntimeConfigInCreateRuntimeMessage
                      )
                    case dataproc: RuntimeConfigRequest.DataprocConfig =>
                      F.pure(
                        RuntimeConfigInCreateRuntimeMessage
                          .fromDataprocInRuntimeConfigRequest(
                            dataproc,
                            config.dataprocConfig.runtimeConfigDefaults
                          ): RuntimeConfigInCreateRuntimeMessage
                      )
                    case gce: RuntimeConfigRequest.GceWithPdConfig =>
                      RuntimeServiceInterp
                        .processPersistentDiskRequest(
                          gce.persistentDisk,
                          gce.zone.getOrElse(config.gceConfig.runtimeConfigDefaults.zone),
                          googleProject,
                          userInfo,
                          userEmail,
                          petSA,
                          FormattedBy.GCE,
                          authProvider,
                          samService,
                          diskConfig,
                          parentWorkspaceId
                        )
                        .map(diskResult =>
                          RuntimeConfigInCreateRuntimeMessage.GceWithPdConfig(
                            gce.machineType.getOrElse(config.gceConfig.runtimeConfigDefaults.machineType),
                            diskResult.disk.id,
                            bootDiskSize,
                            gce.zone.getOrElse(config.gceConfig.runtimeConfigDefaults.zone),
                            gce.gpuConfig
                          ): RuntimeConfigInCreateRuntimeMessage
                        )
                  }
              }

            runtime = convertToRuntime(
              userEmail,
              petSA,
              CloudContext.Gcp(googleProject),
              runtimeName,
              samResource,
              runtimeImages,
              config,
              req,
              context.now,
              parentWorkspaceId
            )

            userScriptUriToValidate = req.userScriptUri
              .flatMap(x => UserScriptPath.gcsPrism.getOption(x).map(_.asString))
            userStartupScriptToValidate = req.startUserScriptUri.flatMap(x =>
              UserScriptPath.gcsPrism.getOption(x).map(_.asString)
            )

            gcsObjectUrisToValidate = runtime.userJupyterExtensionConfig
              .map(config =>
                (config.nbExtensions.values ++ config.serverExtensions.values ++ config.combinedExtensions.values)
                  .filter(_.startsWith("gs://"))
                  .toList
              )
              .getOrElse(List.empty[String]) ++ userScriptUriToValidate ++ userStartupScriptToValidate

            _ <- gcsObjectUrisToValidate
              .parTraverse(s => validateBucketObjectUri(userEmail, petToken, s, context.traceId))
            _ <- context.span.traverse(s => F.delay(s.addAnnotation("Done validating buckets")))
            // Create a notebook-cluster Sam resource with a creator policy and the google project as the parent
            _ <- samService.createResource(
              userInfo.accessToken.token,
              samResource,
              Some(googleProject),
              None,
              getRuntimeSamPolicyMap(userEmail)
            )
            _ <- context.span.traverse(s => F.delay(s.addAnnotation("Done Sam createResource")))
            runtimeConfigToSave = LeoLenses.runtimeConfigPrism.reverseGet(runtimeConfig)
            saveRuntime = SaveCluster(cluster = runtime, runtimeConfig = runtimeConfigToSave, now = context.now)
            runtime <- clusterQuery.save(saveRuntime).transaction
            _ <- publisherQueue.offer(
              CreateRuntimeMessage.fromRuntime(
                runtime,
                runtimeConfig,
                Some(context.traceId),
                req.checkToolsInterruptAfter
              )
            )
          } yield ()
      }
    } yield CreateRuntimeResponse(context.traceId)

  override def getRuntime(userInfo: UserInfo, cloudContext: CloudContext, runtimeName: RuntimeName)(implicit
    as: Ask[F, AppContext]
  ): F[GetRuntimeResponse] =
    for {
      ctx <- as.ask
      // throw 403 if no project-level permission
      hasProjectPermission <- authProvider.isUserProjectReader(
        cloudContext,
        userInfo
      )
      _ <- F.raiseWhen(!hasProjectPermission)(ForbiddenError(userInfo.userEmail, Some(ctx.traceId)))

      // throws 404 if not existent
      resp <- RuntimeServiceDbQueries.getRuntime(cloudContext, runtimeName).transaction

      // throw 404 if no GetClusterStatus permission
      hasPermission <- authProvider.hasPermissionWithProjectFallback[RuntimeSamResourceId, RuntimeAction](
        resp.samResource,
        RuntimeAction.GetRuntimeStatus,
        ProjectAction.GetRuntimeStatus,
        userInfo,
        GoogleProject(cloudContext.asString)
      )
      _ <-
        if (hasPermission) F.unit
        else
          F.raiseError[Unit](
            RuntimeNotFoundException(cloudContext, runtimeName, "permission denied")
          )
    } yield resp

  override def listRuntimes(userInfo: UserInfo, cloudContext: Option[CloudContext], params: Map[String, String])(
    implicit as: Ask[F, AppContext]
  ): F[Vector[ListRuntimeResponse2]] =
    for {
      ctx <- as.ask

      // throw 403 if user doesn't have project permission
      hasProjectPermission <- cloudContext.traverse(cc =>
        authProvider.isUserProjectReader(
          cc,
          userInfo
        )
      )
      _ <- ctx.span.traverse(s => F.delay(s.addAnnotation("Done checking project permission with Sam")))

      _ <- F.raiseWhen(!hasProjectPermission.getOrElse(true))(ForbiddenError(userInfo.userEmail, Some(ctx.traceId)))

      (labelMap, includeDeleted, _) <- F.fromEither(processListParameters(params))
      excludeStatuses = if (includeDeleted) List.empty else List(RuntimeStatus.Deleted)
      creatorOnly <- F.fromEither(processCreatorOnlyParameter(userInfo.userEmail, params, ctx.traceId))

      authorizedIds <- getAuthorizedIds(userInfo, creatorOnly)
      _ <- ctx.span.traverse(s => F.delay(s.addAnnotation("Start DB query for listRuntimes")))
      runtimes <- RuntimeServiceDbQueries
        .listRuntimes(
          // Authorization scopes
          ownerGoogleProjectIds = authorizedIds.ownerGoogleProjectIds,
          ownerWorkspaceIds = authorizedIds.ownerWorkspaceIds,
          readerGoogleProjectIds = authorizedIds.readerGoogleProjectIds,
          readerRuntimeIds = authorizedIds.readerRuntimeIds,
          readerWorkspaceIds = authorizedIds.readerWorkspaceIds,
          // Filters
          excludeStatuses = excludeStatuses,
          creatorEmail = creatorOnly,
          cloudContext = cloudContext,
          labelMap = labelMap
        )
        .transaction

    } yield runtimes

  override def deleteRuntime(req: DeleteRuntimeRequest)(implicit ev: Ask[F, AppContext]): F[Unit] =
    for {
      ctx <- ev.ask
      cloudContext = CloudContext.Gcp(req.googleProject)

      // Resolve the user email in Sam from the user token. This translates a pet token to the owner email.
      userEmail <- samService.getUserEmail(req.userInfo.accessToken.token)

      // throw 403 if no project-level permission
      hasProjectPermission <- authProvider.isUserProjectReader(
        cloudContext,
        req.userInfo
      )
      _ <- F.raiseWhen(!hasProjectPermission)(ForbiddenError(userEmail, Some(ctx.traceId)))

      // throw 404 if not existent
      runtimeOpt <- clusterQuery.getActiveClusterByNameMinimal(cloudContext, req.runtimeName).transaction
      _ <- ctx.span.traverse(s => F.delay(s.addAnnotation("DB | Done getActiveClusterByNameMinimal")))
      runtime <- runtimeOpt.fold(
        F.raiseError[Runtime](RuntimeNotFoundException(cloudContext, req.runtimeName, "no record in database"))
      )(F.pure)
      // throw 404 if no GetClusterStatus permission
      // Note: the general pattern is to 404 (e.g. pretend the runtime doesn't exist) if the caller doesn't have
      // GetClusterStatus permission. We return 403 if the user can view the runtime but can't perform some other action.
      listOfPermissions <- authProvider.getActionsWithProjectFallback(
        runtime.samResource,
        req.googleProject,
        req.userInfo
      )
      hasStatusPermission = listOfPermissions._1.toSet.contains(RuntimeAction.GetRuntimeStatus) ||
        listOfPermissions._2.contains(ProjectAction.GetRuntimeStatus)

      _ <- ctx.span.traverse(s => F.delay(s.addAnnotation("Sam | Done get list of allowed actions")))

      _ <-
        if (hasStatusPermission) F.unit
        else
          log.info(ctx.loggingCtx)(s"${userEmail.value} has no permission to get runtime status") >> F
            .raiseError[Unit](
              RuntimeNotFoundException(
                cloudContext,
                req.runtimeName,
                "no active runtime record in database",
                Some(ctx.traceId)
              )
            )

      // throw 403 if no DeleteCluster permission
      hasDeletePermission = listOfPermissions._1.toSet.contains(RuntimeAction.DeleteRuntime) ||
        listOfPermissions._2.contains(ProjectAction.DeleteRuntime)

      _ <- if (hasDeletePermission) F.unit else F.raiseError[Unit](ForbiddenError(userEmail))
      // throw 409 if the cluster is not deletable
      _ <-
        if (runtime.status.isDeletable) F.unit
        else
          F.raiseError[Unit](
            RuntimeCannotBeDeletedException(cloudContext, runtime.runtimeName, runtime.status)
          )
      // delete the runtime
      runtimeConfig <- RuntimeConfigQueries.getRuntimeConfig(runtime.runtimeConfigId).transaction
      persistentDiskToDelete <- runtimeConfig match {
        case x: RuntimeConfig.GceWithPdConfig =>
          x.persistentDiskId.flatTraverse[F, DiskId] { diskId =>
            for {
              diskOpt <- persistentDiskQuery.getPersistentDiskRecord(diskId).transaction
              disk <- F.fromEither(
                diskOpt.toRight(new RuntimeException(s"Can't find ${diskId} in PERSISTENT_DISK table"))
              )
              detachOp <- googleComputeService.get.detachDisk(
                req.googleProject,
                disk.zone,
                InstanceName(runtime.runtimeName.asString),
                config.gceConfig.userDiskDeviceName
              )
              _ <- detachOp.traverse { op =>
                F.blocking(op.get()).void.recoverWith { case e: java.util.concurrent.ExecutionException =>
                  if (e.getMessage.contains("Not Found"))
                    log.info(ctx.loggingCtx)("Fail to detach disk because the runtime doesn't exist")
                  else F.raiseError(e)
                }
              }
              _ <- RuntimeConfigQueries.updatePersistentDiskId(runtime.runtimeConfigId, None, ctx.now).transaction
              res <-
                if (req.deleteDisk)
                  persistentDiskQuery.updateStatus(diskId, DiskStatus.Deleting, ctx.now).transaction.as(Some(diskId))
                else F.pure(none[DiskId])
            } yield res
          }
        case _ => F.pure(none[DiskId])
      }

      // If there are no async runtime fields defined, we can assume that the underlying runtime
      // has already been deleted. So we just transition the runtime to Deleted status without
      // sending a message to Back Leo.
      //
      // Note this has the side effect of not deleting the disk if requested to do so. The
      // caller must manually delete the disk in this situation. We have the same behavior for apps.
      _ <-
        if (runtime.asyncRuntimeFields.isDefined) {
          clusterQuery.updateClusterStatus(runtime.id, RuntimeStatus.PreDeleting, ctx.now).transaction >> publisherQueue
            .offer(
              DeleteRuntimeMessage(runtime.id, persistentDiskToDelete, Some(ctx.traceId))
            )
        } else {
          clusterQuery.completeDeletion(runtime.id, ctx.now).transaction.void >> samService.deleteResource(
            req.userInfo.accessToken.token,
            runtime.samResource
          )
        }
    } yield ()

  def deleteAllRuntimes(userInfo: UserInfo, cloudContext: CloudContext.Gcp, deleteDisk: Boolean)(implicit
    as: Ask[F, AppContext]
  ): F[Option[Vector[DiskId]]] =
    for {
      ctx <- as.ask
      runtimes <- listRuntimes(userInfo, Some(cloudContext), Map.empty)

      // Extracts all the disks attached to the runtimes so we can differentiate them from orphaned disks when calling the deleteAllResources method
      attachedPersistentDiskIds = runtimes
        .map(r => r.runtimeConfig)
        .traverse(c =>
          c match {
            case x: RuntimeConfig.GceWithPdConfig => x.persistentDiskId
            case _                                => none
          }
        )

      nonDeletableRuntimes = runtimes.filterNot(r => r.status.isDeletable)

      _ <- F
        .raiseError(
          NonDeletableRuntimesInProjectFoundException(
            cloudContext.value,
            nonDeletableRuntimes,
            ctx.traceId
          )
        )
        .whenA(!nonDeletableRuntimes.isEmpty)

      _ <- runtimes
        .traverse(runtime =>
          deleteRuntime(DeleteRuntimeRequest(userInfo, cloudContext.value, runtime.clusterName, deleteDisk))
        )
    } yield attachedPersistentDiskIds

  def deleteRuntimeRecords(userInfo: UserInfo, cloudContext: CloudContext.Gcp, runtime: ListRuntimeResponse2)(implicit
    as: Ask[F, AppContext]
  ): F[Unit] =
    for {
      ctx <- as.ask

      listOfPermissions <- authProvider.getActionsWithProjectFallback(runtime.samResource, cloudContext.value, userInfo)
      // throw 404 if no GetRuntime permission
      hasStatusPermission = listOfPermissions._1.toSet.contains(RuntimeAction.GetRuntimeStatus) ||
        listOfPermissions._2.contains(ProjectAction.GetRuntimeStatus)
      _ <-
        if (hasStatusPermission) F.unit
        else
          F.raiseError[Unit](
            RuntimeNotFoundException(cloudContext, runtime.clusterName, "Permission Denied", Some(ctx.traceId))
          )

      // throw 403 if no DeleteApp permission
      hasDeletePermission = listOfPermissions._1.toSet.contains(RuntimeAction.DeleteRuntime) ||
        listOfPermissions._2.contains(ProjectAction.DeleteRuntime)
      _ <- if (hasDeletePermission) F.unit else F.raiseError[Unit](ForbiddenError(userInfo.userEmail))

      // Mark the resource as deleted in Leo's DB
      _ <- dbReference.inTransaction(clusterQuery.completeDeletion(runtime.id, ctx.now))
      // Delete the notebook-cluster Sam resource
      _ <- samService.deleteResource(userInfo.accessToken.token, runtime.samResource)
    } yield ()

  def deleteAllRuntimesRecords(userInfo: UserInfo, cloudContext: CloudContext.Gcp)(implicit
    as: Ask[F, AppContext]
  ): F[Unit] =
    for {
      ctx <- as.ask
      runtimes <- listRuntimes(userInfo, Some(cloudContext), Map.empty)
      _ <- runtimes.traverse(runtime => deleteRuntimeRecords(userInfo, cloudContext, runtime))
    } yield ()

  def stopRuntime(userInfo: UserInfo, cloudContext: CloudContext, runtimeName: RuntimeName)(implicit
    as: Ask[F, AppContext]
  ): F[Unit] =
    for {
      ctx <- as.ask
      // throw 403 if no project-level permission
      hasProjectPermission <- authProvider.isUserProjectReader(
        cloudContext,
        userInfo
      )
      _ <- F.raiseWhen(!hasProjectPermission)(ForbiddenError(userInfo.userEmail, Some(ctx.traceId)))

      googleProject <- F.fromOption(
        LeoLenses.cloudContextToGoogleProject.get(cloudContext),
        AzureUnimplementedException("Azure runtime is not supported yet")
      )
      // throw 404 if not existent
      runtimeOpt <- clusterQuery
        .getActiveClusterByNameMinimal(cloudContext, runtimeName)(scala.concurrent.ExecutionContext.global)
        .transaction
      _ <- ctx.span.traverse(s => F.delay(s.addAnnotation("Finish query for active runtime")))
      runtime <- runtimeOpt.fold(
        F.raiseError[Runtime](
          RuntimeNotFoundException(cloudContext, runtimeName, "no active runtime found in database")
        )
      )(F.pure)
      // throw 404 if no GetClusterStatus permission
      // Note: the general pattern is to 404 (e.g. pretend the runtime doesn't exist) if the caller doesn't have
      // GetClusterStatus permission. We return 403 if the user can view the runtime but can't perform some other action.

      listOfPermissions <- authProvider.getActionsWithProjectFallback(runtime.samResource, googleProject, userInfo)

      hasStatusPermission = listOfPermissions._1.toSet.contains(RuntimeAction.GetRuntimeStatus) ||
        listOfPermissions._2.contains(ProjectAction.GetRuntimeStatus)

      _ <- ctx.span.traverse(s => F.delay(s.addAnnotation("Sam | Done get list of allowed actions")))

      _ <-
        if (hasStatusPermission) F.unit
        else
          F.raiseError[Unit](
            RuntimeNotFoundException(
              cloudContext,
              runtimeName,
              "GetRuntimeStatus permission is required for stopRuntime"
            )
          )

      // throw 403 if no StopStartCluster permission
      hasStopPermission = listOfPermissions._1.toSet.contains(RuntimeAction.StopStartRuntime) ||
        listOfPermissions._2.contains(ProjectAction.StopStartRuntime)

      _ <- if (hasStopPermission) F.unit else F.raiseError[Unit](ForbiddenError(userInfo.userEmail))
      // throw 409 if the cluster is not stoppable

      _ <-
        if (runtime.status.isStopping) F.unit
        else if (runtime.status.isStoppable) {
          for {
            _ <- clusterQuery.updateClusterStatus(runtime.id, RuntimeStatus.PreStopping, ctx.now).transaction
            _ <- publisherQueue.offer(StopRuntimeMessage(runtime.id, Some(ctx.traceId)))

          } yield ()
        } else
          F.raiseError[Unit](RuntimeCannotBeStoppedException(cloudContext, runtime.runtimeName, runtime.status))
    } yield ()

  def startRuntime(userInfo: UserInfo, googleProject: GoogleProject, runtimeName: RuntimeName)(implicit
    as: Ask[F, AppContext]
  ): F[Unit] =
    for {
      ctx <- as.ask
      // TODO: take cloudContext directly instead of googleProject once we start supporting patching an Azure VM
      cloudContext = CloudContext.Gcp(googleProject)

      // throw 403 if no project-level permission
      hasProjectPermission <- authProvider.isUserProjectReader(
        cloudContext,
        userInfo
      )
      _ <- F.raiseWhen(!hasProjectPermission)(ForbiddenError(userInfo.userEmail, Some(ctx.traceId)))

      // throw 404 if not existent
      runtimeOpt <- clusterQuery
        .getActiveClusterByNameMinimal(cloudContext, runtimeName)(scala.concurrent.ExecutionContext.global)
        .transaction
      _ <- ctx.span.traverse(s => F.delay(s.addAnnotation("Done query for active runtime")))
      runtime <- runtimeOpt.fold(
        F.raiseError[Runtime](RuntimeNotFoundException(cloudContext, runtimeName, "no record in database"))
      )(F.pure)
      // throw 404 if no GetClusterStatus permission
      // Note: the general pattern is to 404 (e.g. pretend the runtime doesn't exist) if the caller doesn't have
      // GetClusterStatus permission. We return 403 if the user can view the runtime but can't perform some other action.

      listOfPermissions <- authProvider.getActionsWithProjectFallback(runtime.samResource, googleProject, userInfo)

      hasStatusPermission = listOfPermissions._1.toSet.contains(RuntimeAction.GetRuntimeStatus) ||
        listOfPermissions._2.contains(ProjectAction.GetRuntimeStatus)

      _ <-
        if (hasStatusPermission) F.unit
        else
          F.raiseError[Unit](
            RuntimeNotFoundException(cloudContext, runtimeName, "GetRuntimeStatus permission is required")
          )

      _ <- ctx.span.traverse(s => F.delay(s.addAnnotation("Sam | Done get list of allowed actions")))

      hasStartPermission = listOfPermissions._1.toSet.contains(RuntimeAction.StopStartRuntime) ||
        listOfPermissions._2.toSet.contains(ProjectAction.StopStartRuntime)

      // throw 403 if no StopStartCluster permission
      _ <- if (hasStartPermission) F.unit else F.raiseError[Unit](ForbiddenError(userInfo.userEmail))

      // throw 409 if the cluster is not startable
      _ <-
        if (runtime.status.isStartable) F.unit
        else
          F.raiseError[Unit](RuntimeCannotBeStartedException(cloudContext, runtime.runtimeName, runtime.status))
      // start the runtime
      ctx <- as.ask
      _ <- clusterQuery.updateClusterStatus(runtime.id, RuntimeStatus.PreStarting, ctx.now).transaction
      _ <- publisherQueue.offer(StartRuntimeMessage(runtime.id, Some(ctx.traceId)))
    } yield ()

  override def updateRuntime(
    userInfo: UserInfo,
    googleProject: GoogleProject,
    runtimeName: RuntimeName,
    req: UpdateRuntimeRequest
  )(implicit as: Ask[F, AppContext]): F[Unit] =
    for {
      ctx <- as.ask
      // TODO: take cloudContext directly instead of googleProject once we start supporting patching an Azure VM
      cloudContext = CloudContext.Gcp(googleProject)

      // throw 403 if no project-level permission
      hasProjectPermission <- authProvider.isUserProjectReader(
        cloudContext,
        userInfo
      )
      _ <- F.raiseWhen(!hasProjectPermission)(ForbiddenError(userInfo.userEmail, Some(ctx.traceId)))

      // throw 404 if not existent
      runtimeOpt <- clusterQuery.getActiveClusterRecordByName(cloudContext, runtimeName).transaction
      runtime <- runtimeOpt.fold(
        F.raiseError[ClusterRecord](RuntimeNotFoundException(cloudContext, runtimeName, "no record in database"))
      )(F.pure)
      // throw 404 if no GetClusterStatus permission
      // Note: the general pattern is to 404 (e.g. pretend the runtime doesn't exist) if the caller doesn't have
      // GetClusterStatus permission. We return 403 if the user can view the runtime but can't perform some other action.

      listOfPermissions <- authProvider.getActionsWithProjectFallback(
        RuntimeSamResourceId(runtime.internalId),
        googleProject,
        userInfo
      )

      hasStatusPermission = listOfPermissions._1.toSet.contains(RuntimeAction.GetRuntimeStatus) ||
        listOfPermissions._2.contains(ProjectAction.GetRuntimeStatus)

      _ <-
        if (hasStatusPermission) F.unit
        else
          F.raiseError[Unit](
            RuntimeNotFoundException(
              cloudContext,
              runtimeName,
              "GetRuntimeStatus permission is required for update runtime"
            )
          )

      // throw 403 if no ModifyCluster permission
      hasModifyPermission = listOfPermissions._1.toSet.contains(RuntimeAction.ModifyRuntime)

      _ <- if (hasModifyPermission) F.unit else F.raiseError[Unit](ForbiddenError(userInfo.userEmail))
      // throw 409 if the cluster is not updatable
      _ <-
        if (runtime.status.isUpdatable) F.unit
        else
          F.raiseError[Unit](RuntimeCannotBeUpdatedException(runtime.projectNameString, runtime.status))
      runtimeConfig <- RuntimeConfigQueries.getRuntimeConfig(runtime.runtimeConfigId).transaction
      // Updating autopause is just a DB update, so we can do it here instead of sending a PubSub message
      updatedAutopauseThreshold = calculateAutopauseThreshold(
        req.updateAutopauseEnabled,
        req.updateAutopauseThreshold.map(_.toMinutes.toInt),
        config.autoFreezeConfig
      )
      _ <-
        if (updatedAutopauseThreshold != runtime.autopauseThreshold)
          clusterQuery.updateAutopauseThreshold(runtime.id, updatedAutopauseThreshold, ctx.now).transaction.void
        else Async[F].unit

      _ <- DBIOAction
        .seq(
          labelQuery.deleteForResource(runtime.id, LabelResourceType.runtime, req.labelsToDelete),
          req.labelsToUpsert.toList.traverse_ { case (k, v) =>
            labelQuery.save(runtime.id, LabelResourceType.runtime, k, v)
          }
        )
        .transaction

      // Updating the runtime config will potentially generate a PubSub message
      _ <- req.updatedRuntimeConfig.traverse_(update =>
        processUpdateRuntimeConfigRequest(update, req.allowStop, runtime, runtimeConfig)
      )
    } yield ()

  private[service] def getRuntimeImages(
    petToken: Option[String],
    now: Instant,
    toolDockerImage: Option[ContainerImage],
    welderRegistry: Option[ContainerRegistry]
  )(implicit ev: Ask[F, TraceId]): F[Set[RuntimeImage]] =
    for {
      // Try to autodetect the image
      autodetectedImageOpt <- toolDockerImage.traverse(image => dockerDAO.detectTool(image, petToken, now))
      // Figure out the tool image. Rules:
      // - if we were able to autodetect an image, use that
      // - else use the default jupyter image
      defaultJupyterImage = RuntimeImage(
        Jupyter,
        config.imageConfig.jupyterImage.imageUrl,
        Some(config.imageConfig.defaultJupyterUserHome),
        now
      )
      toolImage = autodetectedImageOpt getOrElse defaultJupyterImage
      // Figure out the welder image. Rules:
      // - If present, we will use the client-supplied image.
      // - Otherwise we will pull the latest from the specified welderRegistry.
      // - If welderRegistry is undefined, we take the default GCR image from config.
      welderImage = Some(
        RuntimeImage(
          Welder,
          welderRegistry match {
            case Some(ContainerRegistry.DockerHub) => config.imageConfig.welderDockerHubImage.imageUrl
            case _                                 => config.imageConfig.welderGcrImage.imageUrl
          },
          None,
          now
        )
      )

      // Get the proxy image
      proxyImage = RuntimeImage(Proxy, config.imageConfig.proxyImage.imageUrl, None, now)
      // Crypto detector image - note it's not currently supported on Dockerhub
      cryptoDetectorImageOpt = welderRegistry match {
        case Some(ContainerRegistry.DockerHub) => None
        case _ => Some(RuntimeImage(CryptoDetector, config.imageConfig.cryptoDetectorImage.imageUrl, None, now))
      }
    } yield Set(Some(toolImage), welderImage, Some(proxyImage), cryptoDetectorImageOpt).flatten

  private[service] def validateBucketObjectUri(
    userEmail: WorkbenchEmail,
    userToken: String,
    gcsUri: String,
    traceId: TraceId
  ): F[Unit] = {
    val gcsUriOpt = google.parseGcsPath(gcsUri)
    gcsUriOpt match {
      case Left(e) => F.raiseError(BucketObjectException(gcsUri, e.value))
      case Right(gcsPath) if gcsPath.toUri.length > bucketPathMaxLength =>
        F.raiseError(BucketObjectException(gcsUri, s"bucket path longer than ${bucketPathMaxLength} is prohibited"))
      case Right(gcsPath) =>
        // Retry 401s from Google here because they can be thrown spuriously with valid credentials.
        // See https://github.com/DataBiosphere/leonardo/issues/460
        // Note GoogleStorageDAO already retries 500 and other errors internally, so we just need to catch 401s here.
        // We might think about moving the retry-on-401 logic inside GoogleStorageDAO.
        val accessToken = new AccessToken(userToken, null) // we currently don't get expiration time from sam
        val credentials = GoogleCredentials.create(accessToken)
        val retryPolicy = RetryPredicates.retryConfigWithPredicates(
          RetryPredicates.standardGoogleRetryPredicate,
          RetryPredicates.whenStatusCode(401)
        )

        val res = for {
          blob <- googleStorageService.get
            .getBlob(
              gcsPath.bucketName,
              GcsBlobName(gcsPath.objectName.value),
              credential = Some(credentials),
              Some(traceId),
              retryPolicy
            )
            .compile
            .last
          _ <-
            if (blob.isDefined) F.unit
            else F.raiseError[Unit](BucketObjectException(gcsUri, "bucket doesn't exist in Google"))
        } yield ()

        res.recoverWith {
          case e: BaseServiceException if e.getCode == StatusCodes.Forbidden.intValue =>
            log.error(e)(
              s"User ${userEmail.value}'s PET account does not have access to ${gcsPath.bucketName} / ${gcsPath.objectName}"
            ) >> F.raiseError[Unit](BucketObjectAccessException(userEmail, gcsPath))
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
  def processUpdateRuntimeConfigRequest(
    request: UpdateRuntimeConfigRequest,
    allowStop: Boolean,
    runtime: ClusterRecord,
    runtimeConfig: RuntimeConfig
  )(implicit ctx: Ask[F, AppContext]): F[Unit] =
    for {
      context <- ctx.ask
      msg <- (runtimeConfig, request) match {
        case (
              RuntimeConfig.GceConfig(machineType, existngDiskSize, _, _, _),
              UpdateRuntimeConfigRequest.GceConfig(newMachineType, diskSizeInRequest)
            ) =>
          for {
            targetDiskSize <- traverseIfChanged(diskSizeInRequest, existngDiskSize) { d =>
              if (d.gb < existngDiskSize.gb)
                F.raiseError[DiskUpdate](
                  RuntimeDiskSizeCannotBeDecreasedException(runtime.projectNameString)
                )
              else
                F.pure(DiskUpdate.NoPdSizeUpdate(d): DiskUpdate)
            }
            r <- processUpdateGceConfigRequest(
              newMachineType,
              allowStop,
              runtime,
              machineType,
              targetDiskSize,
              context.traceId
            )
          } yield r
        case (
              RuntimeConfig.GceWithPdConfig(machineType, diskIdOpt, _, _, _),
              UpdateRuntimeConfigRequest.GceConfig(newMachineType, diskSizeInRequest)
            ) =>
          for {
            // should disk size be updated?
            diskId <- F.fromEither(
              diskIdOpt.toRight(RuntimeDiskNotFound(runtime.cloudContext, runtime.runtimeName, context.traceId))
            )
            diskOpt <- persistentDiskQuery.getById(diskId).transaction
            disk <- F.fromEither(diskOpt.toRight(DiskNotFoundByIdException(diskId, context.traceId)))
            diskUpdate <- traverseIfChanged(diskSizeInRequest, disk.size) { d =>
              if (d.gb < disk.size.gb)
                Async[F].raiseError[DiskUpdate](RuntimeDiskSizeCannotBeDecreasedException(runtime.projectNameString))
              else if (!allowStop)
                Async[F].raiseError[DiskUpdate](RuntimeDiskSizeCannotBeChangedException(runtime.projectNameString))
              else
                Async[F].pure(DiskUpdate.PdSizeUpdate(disk.id, disk.name, d): DiskUpdate)
            }
            r <- processUpdateGceConfigRequest(
              newMachineType,
              allowStop,
              runtime,
              machineType,
              diskUpdate,
              context.traceId
            )
          } yield r
        case (
              dataprocConfig @ RuntimeConfig.DataprocConfig(_, _, _, _, _, _, _, _, _, _, _),
              req @ UpdateRuntimeConfigRequest.DataprocConfig(_, _, _, _)
            ) =>
          processUpdateDataprocConfigRequest(req, allowStop, runtime, dataprocConfig)

        case _ =>
          Async[F].raiseError[Option[UpdateRuntimeMessage]](
            WrongCloudServiceException(runtimeConfig.cloudService, request.cloudService, context.traceId)
          )
      }
      _ <- msg.traverse_ { m =>
        if (m.stopToUpdateMachineType) {
          val patchDetails = RuntimePatchDetails(runtime.id, RuntimeStatus.Stopped)
          patchQuery.save(patchDetails, m.newMachineType).transaction.void >> metrics.incrementCounter(
            "patchStopToUpdate"
          )
        } else F.unit
      }
    } yield ()

  private[service] def processUpdateGceConfigRequest(
    newMachineType: Option[MachineTypeName],
    allowStop: Boolean,
    runtime: ClusterRecord,
    existingMachineType: MachineTypeName,
    targetDiskSize: Option[DiskUpdate],
    traceId: TraceId
  ): F[Option[UpdateRuntimeMessage]] =
    for {
      // should machine type be updated?
      targetMachineType <- getTargetMachineType(
        existingMachineType,
        newMachineType,
        runtime.projectNameString,
        runtime.status,
        allowStop
      )
      // if either of the above is defined, send a PubSub message
      msg <- (targetMachineType orElse targetDiskSize).traverse { _ =>
        val requiresRestart = targetMachineType.exists(x => x._2) || targetDiskSize.isDefined

        val message = UpdateRuntimeMessage(
          runtime.id,
          targetMachineType.map(_._1),
          requiresRestart,
          targetDiskSize,
          None,
          None,
          Some(traceId)
        )
        publisherQueue.offer(message).as(message)
      }
      // we only need to return the message that might cause a start/stop transition
    } yield msg

  private[service] def processUpdateDataprocConfigRequest(
    req: UpdateRuntimeConfigRequest.DataprocConfig,
    allowStop: Boolean,
    runtime: ClusterRecord,
    dataprocConfig: RuntimeConfig.DataprocConfig
  )(implicit ctx: Ask[F, AppContext]): F[Option[UpdateRuntimeMessage]] =
    for {
      context <- ctx.ask
      // should num workers be updated?
      targetNumWorkers <- traverseIfChanged(req.updatedNumberOfWorkers, dataprocConfig.numberOfWorkers)(Async[F].pure)
      _ <- targetNumWorkers.traverse(_ =>
        runtime.status match {
          case RuntimeStatus.Running =>
            F.unit
          case s =>
            F.raiseError[Unit](
              new LeoException(
                s"Bad request. Number of workers can only be updated if the dataproc cluster is Running. Cluster is in ${s} currently",
                StatusCodes.BadRequest,
                traceId = Some(context.traceId)
              )
            )
        }
      )
      // should num preemptibles be updated?
      targetNumPreemptibles <- traverseIfChanged(
        req.updatedNumberOfPreemptibleWorkers,
        dataprocConfig.numberOfPreemptibleWorkers.getOrElse(0)
      )(Async[F].pure)
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
            Async[F].raiseError[(MachineTypeName, Boolean)](
              RuntimeMachineTypeCannotBeChangedException(runtime.projectNameString, runtime.status)
            )
          else Async[F].pure((mt, true))
      }
      mainInstance <- instanceQuery.getMasterForCluster(runtime.id).transaction
      // should master disk size be updated?
      targetMasterDiskSize <- traverseIfChanged(req.updatedMasterDiskSize, dataprocConfig.masterDiskSize) { d =>
        if (d.gb < dataprocConfig.masterDiskSize.gb)
          Async[F].raiseError[DiskUpdate](RuntimeDiskSizeCannotBeDecreasedException(runtime.projectNameString))
        else
          Async[F].pure(
            DiskUpdate.Dataproc(d, mainInstance): DiskUpdate
          )
      }

      // if any of the above is defined, send a PubSub message
      msg <-
        if (
          List(targetNumWorkers, targetNumPreemptibles, targetMasterMachineType, targetMasterDiskSize).exists(
            _.isDefined
          )
        ) {
          val requiresRestart = targetMasterMachineType.exists(x => x._2) || targetMasterDiskSize.isDefined
          val message = UpdateRuntimeMessage(
            runtime.id,
            targetMasterMachineType.map(_._1),
            requiresRestart,
            targetMasterDiskSize,
            targetNumWorkers,
            targetNumPreemptibles,
            Some(context.traceId)
          )
          publisherQueue.offer(message).as(Some(message))
        } else F.pure(none[UpdateRuntimeMessage])
    } yield msg

  private[service] def getTargetMachineType(
    curMachineType: MachineTypeName,
    reqMachineType: Option[MachineTypeName],
    projectNameString: String,
    status: RuntimeStatus,
    allowStop: Boolean
  ) =
    for {
      targetMachineType <- traverseIfChanged(reqMachineType, curMachineType) { mt =>
        if (status == RuntimeStatus.Stopped)
          Async[F].pure((mt, false))
        else if (!allowStop)
          Async[F].raiseError[(MachineTypeName, Boolean)](
            RuntimeMachineTypeCannotBeChangedException(projectNameString, status)
          )
        else Async[F].pure((mt, true))
      }
    } yield targetMachineType

  private[service] def getAuthorizedIds(
    userInfo: UserInfo,
    creatorEmail: Option[WorkbenchEmail] = None
  )(implicit ev: Ask[F, AppContext]): F[AuthorizedIds] = for {
    // Authorize: user has an active account and has accepted terms of service
    _ <- authProvider.checkUserEnabled(userInfo)

    // Authorize: get resource IDs the user can see
    // HACK: leonardo is modeling access control here, handling inheritance
    // of workspace and project-level permissions. Sam and WSM already do this,
    // and should be considered the point of truth.

    // HACK: leonardo short-circuits access control to grant access to runtime creators.
    // This supports the use case where `terra-ui` requests status of runtimes that have
    // not yet been provisioned in Sam.
    creatorRuntimeIdsBackdoor: Set[RuntimeSamResourceId] <- creatorEmail match {
      case Some(email: WorkbenchEmail) =>
        RuntimeServiceDbQueries
          .listRuntimeIdsForCreator(email)
          .map(_.map(_.samResource).toSet)
          .transaction
      case None => F.pure(Set.empty: Set[RuntimeSamResourceId])
    }

    // v1 runtimes (sam resource type `notebook-cluster`) are readable only
    // by their creators (`Creator` is the SamResource.Runtime `ownerRoleName`),
    // if the creator also has read access to the corresponding SamResource.Project
    creatorV1RuntimeIds: Set[RuntimeSamResourceId] <- authProvider
      .listResourceIds[RuntimeSamResourceId](hasOwnerRole = true, userInfo)
    readerProjectIds: Set[ProjectSamResourceId] <- authProvider
      .listResourceIds[ProjectSamResourceId](hasOwnerRole = false, userInfo)

    // v1 runtimes are discoverable by owners on the corresponding Project
    ownerProjectIds: Set[ProjectSamResourceId] <- authProvider
      .listResourceIds[ProjectSamResourceId](hasOwnerRole = true, userInfo)

    // combine: to read a runtime, user needs to be at least one of:
    // - creator of a v1 runtime (Sam-authenticated)
    // - any role on a v2 runtime (Sam-authenticated)
    // - creator of a runtime (in Leo db) and filtering their request by creator-only
    readerRuntimeIds: Set[SamResourceId] = creatorV1RuntimeIds ++ creatorRuntimeIdsBackdoor
  } yield AuthorizedIds(
    ownerGoogleProjectIds = ownerProjectIds,
    ownerWorkspaceIds = Set.empty,
    readerGoogleProjectIds = readerProjectIds,
    readerRuntimeIds = readerRuntimeIds,
    readerWorkspaceIds = Set.empty
  )

}

object RuntimeServiceInterp {

  private[service] def getToolFromImages(clusterImages: Set[RuntimeImage]): Option[Tool] =
    clusterImages.map(_.imageType.toString).find(Tool.namesToValuesMap.contains) match {
      case Some(value) => Tool.withNameOption(value)
      case None        => None
    }

  private[service] def convertToRuntime(
    userEmail: WorkbenchEmail,
    serviceAccountInfo: WorkbenchEmail,
    cloudContext: CloudContext,
    runtimeName: RuntimeName,
    clusterInternalId: RuntimeSamResourceId,
    clusterImages: Set[RuntimeImage],
    config: RuntimeServiceConfig,
    req: CreateRuntimeRequest,
    now: Instant,
    workspaceId: Option[WorkspaceId]
  ): Runtime = {
    // create a LabelMap of default labels
    val defaultLabels = DefaultRuntimeLabels(
      runtimeName,
      Some(GoogleProject(cloudContext.asString)),
      cloudContext,
      userEmail,
      Some(serviceAccountInfo),
      req.userScriptUri,
      req.startUserScriptUri,
      getToolFromImages(clusterImages)
    ).toMap

    // combine default and given labels and add labels for extensions
    val allLabels = req.labels ++ defaultLabels ++ req.userJupyterExtensionConfig.map(_.asLabels).getOrElse(Map.empty)

    val autopauseThreshold = calculateAutopauseThreshold(
      req.autopause,
      req.autopauseThreshold.map(_.toMinutes.toInt),
      config.autoFreezeConfig
    ) // TODO: use FiniteDuration for autopauseThreshold field in Cluster
    val clusterScopes = req.runtimeConfig match {
      case Some(rq) =>
        rq.cloudService match {
          case CloudService.GCE =>
            if (req.scopes.isEmpty) config.gceConfig.defaultScopes else req.scopes
          case CloudService.Dataproc =>
            if (req.scopes.isEmpty) config.dataprocConfig.defaultScopes else req.scopes
          case CloudService.AzureVm =>
            Set.empty[String] // Doesn't apply to Azure

        }
      case None =>
        if (req.scopes.isEmpty) config.gceConfig.defaultScopes
        else req.scopes // default to create gce runtime if runtimeConfig is not specified
    }

    Runtime(
      0,
      workspaceId,
      samResource = clusterInternalId,
      runtimeName = runtimeName,
      cloudContext = cloudContext,
      serviceAccount = serviceAccountInfo,
      asyncRuntimeFields = None,
      auditInfo = AuditInfo(userEmail, now, None, now),
      kernelFoundBusyDate = None,
      proxyUrl = Runtime.getProxyUrl(config.proxyUrlBase, cloudContext, runtimeName, clusterImages, None, allLabels),
      status = RuntimeStatus.PreCreating,
      labels = allLabels,
      userScriptUri = req.userScriptUri,
      startUserScriptUri = req.startUserScriptUri,
      errors = List.empty,
      userJupyterExtensionConfig = req.userJupyterExtensionConfig,
      autopauseThreshold = autopauseThreshold,
      defaultClientId = req.defaultClientId,
      allowStop = false,
      runtimeImages = clusterImages,
      scopes = clusterScopes,
      welderEnabled = true,
      customEnvironmentVariables = req.customEnvironmentVariables,
      runtimeConfigId = RuntimeConfigId(-1),
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
    targetZone: ZoneName,
    googleProject: GoogleProject,
    userInfo: UserInfo,
    userEmail: WorkbenchEmail,
    serviceAccount: WorkbenchEmail,
    willBeUsedBy: FormattedBy,
    authProvider: LeoAuthProvider[F],
    samService: SamService[F],
    diskConfig: PersistentDiskConfig,
    workspaceId: Option[WorkspaceId]
  )(implicit
    as: Ask[F, AppContext],
    F: Async[F],
    dbReference: DbReference[F],
    ec: ExecutionContext
  ): F[PersistentDiskRequestResult] =
    for {
      ctx <- as.ask
      cloudContext = CloudContext.Gcp(googleProject)
      diskOpt <- persistentDiskQuery.getActiveByName(cloudContext, req.name).transaction
      disk <- diskOpt match {
        case Some(pd) =>
          for {
            _ <-
              if (pd.zone == targetZone) F.unit
              else
                F.raiseError(
                  BadRequestException(
                    s"existing disk ${pd.projectNameString} is in zone ${pd.zone.value}, and cannot be attached to a runtime in zone ${targetZone.value}. Please create your runtime in zone ${pd.zone.value} if you'd like to use this disk; or opt to use a new disk",
                    Some(ctx.traceId)
                  )
                )
            isAttached <- pd.formattedBy match {
              case None =>
                for {
                  isAttachedToRuntime <- RuntimeConfigQueries.isDiskAttached(pd.id).transaction
                  isAttached <-
                    if (isAttachedToRuntime) F.pure(true)
                    else appQuery.isDiskAttached(pd.id).transaction
                } yield isAttached
              case Some(formattedBy) =>
                if (willBeUsedBy == formattedBy) {
                  formattedBy match {
                    case FormattedBy.Galaxy | FormattedBy.Cromwell | FormattedBy.Custom | FormattedBy.Allowed |
                        FormattedBy.Jupyter =>
                      appQuery.isDiskAttached(pd.id).transaction
                    case FormattedBy.GCE => RuntimeConfigQueries.isDiskAttached(pd.id).transaction
                  }
                } else
                  F.raiseError[Boolean](
                    DiskAlreadyFormattedByOtherApp(CloudContext.Gcp(googleProject), req.name, ctx.traceId, formattedBy)
                  )
            }
            // throw 409 if the disk is attached to a runtime
            _ <-
              if (isAttached)
                F.raiseError[Unit](DiskAlreadyAttachedException(CloudContext.Gcp(googleProject), req.name, ctx.traceId))
              else F.unit
            hasPermission <- authProvider.hasPermission[PersistentDiskSamResourceId, PersistentDiskAction](
              pd.samResource,
              PersistentDiskAction.AttachPersistentDisk,
              userInfo
            )

            _ <- if (hasPermission) F.unit else F.raiseError[Unit](ForbiddenError(userEmail))
          } yield PersistentDiskRequestResult(pd, false)

        case None =>
          for {
            hasPermission <- authProvider.hasPermission[ProjectSamResourceId, ProjectAction](
              ProjectSamResourceId(googleProject),
              ProjectAction.CreatePersistentDisk,
              userInfo
            )
            _ <- if (hasPermission) F.unit else F.raiseError[Unit](ForbiddenError(userEmail))
            samResource <- F.delay(PersistentDiskSamResourceId(UUID.randomUUID().toString))
            diskBeforeSave <- F.fromEither(
              DiskServiceInterp.convertToDisk(
                userEmail,
                serviceAccount,
                cloudContext,
                req.name,
                samResource,
                diskConfig,
                CreateDiskRequest.fromDiskConfigRequest(req, Some(targetZone)),
                ctx.now,
                willBeUsedBy == FormattedBy.Galaxy,
                None,
                workspaceId
              )
            )
            // Create a persistent-disk Sam resource with a creator policy and the google project as the parent
            _ <- samService.createResource(
              userInfo.accessToken.token,
              samResource,
              Some(googleProject),
              None,
              getDiskSamPolicyMap(userEmail)
            )
            pd <- persistentDiskQuery.save(diskBeforeSave).transaction
          } yield PersistentDiskRequestResult(pd, true)
      }
    } yield disk

  def processPersistentDiskRequestForWorkspace[F[_]](
    req: PersistentDiskRequest,
    targetZone: ZoneName,
    cloudContext: CloudContext,
    workspaceId: WorkspaceId,
    userInfo: UserInfo,
    userEmail: WorkbenchEmail,
    serviceAccount: WorkbenchEmail,
    willBeUsedBy: FormattedBy,
    authProvider: LeoAuthProvider[F],
    samService: SamService[F],
    diskConfig: PersistentDiskConfig
  )(implicit
    as: Ask[F, AppContext],
    F: Async[F],
    dbReference: DbReference[F],
    ec: ExecutionContext
  ): F[PersistentDiskRequestResult] =
    for {
      ctx <- as.ask
      diskOpt <- persistentDiskQuery.getActiveByName(cloudContext, req.name).transaction
      disk <- diskOpt match {
        case Some(pd) =>
          for {
            _ <-
              if (pd.zone == targetZone) F.unit
              else
                F.raiseError(
                  BadRequestException(
                    s"existing disk ${pd.projectNameString} is in zone ${pd.zone.value}, and cannot be attached to a runtime in zone ${targetZone.value}. Please create your runtime in zone ${pd.zone.value} if you'd like to use this disk; or opt to use a new disk",
                    Some(ctx.traceId)
                  )
                )
            isAttached <- pd.formattedBy match {
              case None =>
                for {
                  isAttachedToRuntime <- RuntimeConfigQueries.isDiskAttached(pd.id).transaction
                  isAttached <-
                    if (isAttachedToRuntime) F.pure(true)
                    else appQuery.isDiskAttached(pd.id).transaction
                } yield isAttached
              case Some(formattedBy) =>
                if (willBeUsedBy == formattedBy) {
                  formattedBy match {
                    case FormattedBy.Galaxy | FormattedBy.Cromwell | FormattedBy.Custom | FormattedBy.Allowed |
                        FormattedBy.Jupyter =>
                      appQuery.isDiskAttached(pd.id).transaction
                    case FormattedBy.GCE => RuntimeConfigQueries.isDiskAttached(pd.id).transaction
                  }
                } else
                  F.raiseError[Boolean](
                    DiskAlreadyFormattedByOtherApp(cloudContext, req.name, ctx.traceId, formattedBy)
                  )
            }
            // throw 409 if the disk is attached to a runtime
            _ <-
              if (isAttached)
                F.raiseError[Unit](DiskAlreadyAttachedException(cloudContext, req.name, ctx.traceId))
              else F.unit
            hasPermission <- authProvider.hasPermission[PersistentDiskSamResourceId, PersistentDiskAction](
              pd.samResource,
              PersistentDiskAction.AttachPersistentDisk,
              userInfo
            )

            _ <- if (hasPermission) F.unit else F.raiseError[Unit](ForbiddenError(userEmail))
          } yield PersistentDiskRequestResult(pd, false)

        case None =>
          for {
            hasPermission <- authProvider.hasPermission[WorkspaceResourceSamResourceId, WorkspaceAction](
              WorkspaceResourceSamResourceId(workspaceId),
              WorkspaceAction.CreateControlledApplicationResource,
              userInfo
            ) // TODO: Correct check?
            _ <- if (hasPermission) F.unit else F.raiseError[Unit](ForbiddenError(userEmail))
            samResource <- F.delay(PersistentDiskSamResourceId(UUID.randomUUID().toString))
            diskBeforeSave <- F.fromEither(
              DiskServiceInterp.convertToDisk(
                userEmail,
                serviceAccount,
                cloudContext,
                req.name,
                samResource,
                diskConfig,
                CreateDiskRequest.fromDiskConfigRequest(req, Some(targetZone)),
                ctx.now,
                willBeUsedBy == FormattedBy.Galaxy,
                None,
                Some(workspaceId)
              )
            )
            // Create a persistent-disk Sam resource with a creator policy and the workspace as the parent
            _ <- samService.createResource(userInfo.accessToken.token,
                                           samResource,
                                           None,
                                           Some(workspaceId),
                                           getDiskSamPolicyMap(userEmail)
            )
            pd <- persistentDiskQuery.save(diskBeforeSave).transaction
          } yield PersistentDiskRequestResult(pd, true)
      }
    } yield disk

  private[service] def calculateAutopauseThreshold(
    autopause: Option[Boolean],
    autopauseThreshold: Option[Int],
    autoFreezeConfig: AutoFreezeConfig
  ): Int =
    autopause match {
      case Some(false) =>
        autoPauseOffValue
      case _ =>
        autopauseThreshold match {
          case Some(v) => v
          case None    => autoFreezeConfig.autoFreezeAfter.toMinutes.toInt
        }
    }

  private[service] def getRuntimeSamPolicyMap(userEmail: WorkbenchEmail): Map[String, SamPolicyData] =
    Map("creator" -> SamPolicyData(List(userEmail), List(RuntimeRole.Creator.asString)))
}

final case class PersistentDiskRequestResult(disk: PersistentDisk, creationNeeded: Boolean)

final case class RuntimeServiceConfig(
  proxyUrlBase: String,
  imageConfig: ImageConfig,
  autoFreezeConfig: AutoFreezeConfig,
  dataprocConfig: DataprocConfig,
  gceConfig: GceConfig,
  azureConfig: AzureServiceConfig
)

final case class WrongCloudServiceException(
  runtimeCloudService: CloudService,
  updateCloudService: CloudService,
  traceId: TraceId
) extends LeoException(
      s"Bad request. This runtime is created with ${runtimeCloudService.asString}, and can not be updated to use ${updateCloudService.asString}",
      StatusCodes.Conflict,
      traceId = Some(traceId)
    )

// thrown when a runtime has a GceWithPdConfig but has no PD attached, which should only happen for deleted runtimes
final case class RuntimeDiskNotFound(cloudContext: CloudContext, runtimeName: RuntimeName, traceId: TraceId)
    extends LeoException(
      s"Persistent disk not found for runtime ${cloudContext.asStringWithProvider}/${runtimeName.asString}",
      StatusCodes.NotFound,
      traceId = Some(traceId)
    )

final case class DiskNotSupportedException(traceId: TraceId)
    extends LeoException(
      s"Persistent disks are not supported on Google Cloud Dataproc",
      StatusCodes.Conflict,
      traceId = Some(traceId)
    )

final case class DiskAlreadyAttachedException(cloudContext: CloudContext, name: DiskName, traceId: TraceId)
    extends LeoException(
      s"Your persistent disk ${cloudContext.asStringWithProvider}/${name.value} is already attached to another cloud environment. You might need to wait a few minutes if you just deleted a runtime.",
      StatusCodes.Conflict,
      traceId = Some(traceId)
    )

final case class DiskAlreadyFormattedByOtherApp(
  cloudContext: CloudContext,
  name: DiskName,
  traceId: TraceId,
  formattedBy: FormattedBy
) extends LeoException(
      s"Persistent disk ${cloudContext.asStringWithProvider}/${name.value} is already formatted by ${formattedBy.asString}",
      StatusCodes.Conflict,
      traceId = Some(traceId)
    )
