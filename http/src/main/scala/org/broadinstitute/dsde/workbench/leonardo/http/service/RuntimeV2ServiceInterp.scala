package org.broadinstitute.dsde.workbench.leonardo
package http
package service

import akka.http.scaladsl.model.StatusCodes
import cats.Parallel
import cats.effect.Async
import cats.effect.std.Queue
import cats.mtl.Ask
import cats.syntax.all._
import org.broadinstitute.dsde.workbench.google2.{DiskName, MachineTypeName, ZoneName}
import org.broadinstitute.dsde.workbench.leonardo.JsonCodec._
import org.broadinstitute.dsde.workbench.leonardo.SamResourceId.{
  PersistentDiskSamResourceId,
  ProjectSamResourceId,
  RuntimeSamResourceId,
  WorkspaceResourceSamResourceId,
  WsmResourceSamResourceId
}
import org.broadinstitute.dsde.workbench.leonardo.config.PersistentDiskConfig
import org.broadinstitute.dsde.workbench.leonardo.dao._
import org.broadinstitute.dsde.workbench.leonardo.db._
import org.broadinstitute.dsde.workbench.leonardo.model.SamResourceAction.{
// do not remove `projectSamResourceAction`; it is implicit
  projectSamResourceAction,
// do not remove `runtimeSamResourceAction`; it is implicit
  runtimeSamResourceAction,
// do not remove `workspaceSamResourceAction`; it is implicit
  workspaceSamResourceAction
}
import org.broadinstitute.dsde.workbench.leonardo.model._
import org.broadinstitute.dsde.workbench.leonardo.monitor.LeoPubsubMessage.{
  CreateAzureRuntimeMessage,
  DeleteAzureRuntimeMessage,
  StartRuntimeMessage,
  StopRuntimeMessage
}
import org.broadinstitute.dsde.workbench.leonardo.monitor.{LeoPubsubMessage, UpdateDateAccessMessage}
import org.broadinstitute.dsde.workbench.model.{TraceId, UserInfo, WorkbenchEmail}
import org.http4s.AuthScheme
import org.typelevel.log4cats.StructuredLogger

import java.time.Instant
import java.util.UUID
import scala.concurrent.ExecutionContext
class RuntimeV2ServiceInterp[F[_]: Parallel](config: RuntimeServiceConfig,
                                             authProvider: LeoAuthProvider[F],
                                             wsmDao: WsmDao[F],
                                             samDAO: SamDAO[F],
                                             publisherQueue: Queue[F, LeoPubsubMessage],
                                             dateAccessUpdaterQueue: Queue[F, UpdateDateAccessMessage],
                                             wsmClientProvider: WsmApiClientProvider[F]
)(implicit
  F: Async[F],
  dbReference: DbReference[F],
  ec: ExecutionContext,
  log: StructuredLogger[F]
) extends RuntimeV2Service[F] {
  override def createRuntime(userInfo: UserInfo,
                             runtimeName: RuntimeName,
                             workspaceId: WorkspaceId,
                             useExistingDisk: Boolean,
                             req: CreateAzureRuntimeRequest
  )(implicit as: Ask[F, AppContext]): F[CreateRuntimeResponse] =
    for {
      ctx <- as.ask

      userToken = org.http4s.headers.Authorization(
        org.http4s.Credentials.Token(AuthScheme.Bearer, userInfo.accessToken.token)
      )

      workspaceDescOpt <- wsmDao.getWorkspace(workspaceId, userToken)
      workspaceDesc <- F.fromOption(workspaceDescOpt, WorkspaceNotFoundException(workspaceId, ctx.traceId))

      // TODO: when we fully support google here, do something intelligent instead of defaulting to azure
      cloudContext <- (workspaceDesc.azureContext, workspaceDesc.gcpContext) match {
        case (Some(azureContext), _) => F.pure[CloudContext](CloudContext.Azure(azureContext))
        case (_, Some(gcpContext))   => F.pure[CloudContext](CloudContext.Gcp(gcpContext))
        case (None, None) => F.raiseError[CloudContext](CloudContextNotFoundException(workspaceId, ctx.traceId))
      }

      samResource = WorkspaceResourceSamResourceId(workspaceId)

      hasPermission <- authProvider.hasPermission[WorkspaceResourceSamResourceId, WorkspaceAction](
        samResource,
        WorkspaceAction.CreateControlledUserResource,
        userInfo
      )

      _ <- ctx.span.traverse(s => F.delay(s.addAnnotation("Done auth call for azure runtime permission")))
      _ <- F
        .raiseUnless(hasPermission)(ForbiddenError(userInfo.userEmail))

      storageContainerOpt <- wsmDao.getWorkspaceStorageContainer(workspaceId, userToken)
      storageContainer <- F.fromOption(
        storageContainerOpt,
        BadRequestException(s"Workspace ${workspaceId.value} doesn't have storage container provisioned appropriately",
                            Some(ctx.traceId)
        )
      )

      // enforcing one runtime per workspace/user at a time
      runtime <- RuntimeServiceDbQueries
        .listRuntimesForWorkspace(Map.empty,
                                  List(RuntimeStatus.Deleted, RuntimeStatus.Deleting),
                                  Some(userInfo.userEmail),
                                  Some(workspaceId),
                                  Some(cloudContext.cloudProvider)
        )
        .transaction
      _ <- F
        .raiseError(
          OnlyOneRuntimePerWorkspacePerCreator(workspaceId,
                                               userInfo.userEmail,
                                               runtime.head.clusterName,
                                               runtime.head.status
          )
        )
        .whenA(runtime.length != 0)

      runtimeOpt <- RuntimeServiceDbQueries.getStatusByName(cloudContext, runtimeName).transaction
      _ <- ctx.span.traverse(s => F.delay(s.addAnnotation("Done DB query for azure runtime")))

      // Get the Landing Zone Resources for the app for Azure
      leoAuth <- samDAO.getLeoAuthToken
      landingZoneResources <- cloudContext.cloudProvider match {
        case CloudProvider.Gcp =>
          F.raiseError(
            BadRequestException(s"Workspace ${workspaceId.value} is GCP and doesn't support V2 VM creation",
                                Some(ctx.traceId)
            )
          )
        case CloudProvider.Azure => wsmDao.getLandingZoneResources(workspaceDesc.spendProfile, leoAuth)
      }

      runtimeImage: RuntimeImage = RuntimeImage(
        RuntimeImageType.Azure,
        config.azureConfig.image.asString,
        None,
        ctx.now
      )
      listenerImage: RuntimeImage = RuntimeImage(
        RuntimeImageType.Listener,
        config.azureConfig.listenerImage,
        None,
        ctx.now
      )
      welderImage: RuntimeImage = RuntimeImage(
        RuntimeImageType.Welder,
        config.azureConfig.welderImage,
        None,
        ctx.now
      )
      vmSamResourceId <- F.delay(UUID.randomUUID())

      _ <- runtimeOpt match {
        case Some(status) => F.raiseError[Unit](RuntimeAlreadyExistsException(cloudContext, runtimeName, status))
        case None =>
          for {
            diskId <- useExistingDisk match {

              // if using existing disk, find disk in users workspace
              case true =>
                for {
                  disks <- DiskServiceDbQueries
                    .listDisks(Map.empty,
                               includeDeleted = false,
                               Some(userInfo.userEmail),
                               Some(cloudContext),
                               Some(workspaceId)
                    )
                    .transaction
                  // check if 0 or multiple disks
                  disk <- disks.length match {
                    case 1 => F.pure(disks.head)
                    case 0 => F.raiseError(NoPersistentDiskException(workspaceId))
                    case _ =>
                      F.raiseError(MultiplePersistentDisksException(workspaceId, disks.length, disks))
                  }
                  // check disk is ready
                  _ <- F
                    .raiseError(PersistentDiskNotReadyException(disk.id, disk.status))
                    .whenA(disk.status != DiskStatus.Ready)
                  isAttached <- persistentDiskQuery.isDiskAttached(disk.id).transaction
                  _ <- F
                    .raiseError(DiskAlreadyAttachedException(disk.cloudContext, disk.name, ctx.traceId))
                    .whenA(isAttached)
                } yield disk.id

              // if not using existing disk, create a new one
              case false =>
                for {
                  samResource <- F.delay(PersistentDiskSamResourceId(UUID.randomUUID().toString))
                  pd <- F.fromEither(
                    convertToDisk(
                      userInfo,
                      cloudContext,
                      DiskName(req.azureDiskConfig.name.value),
                      samResource,
                      config.azureConfig.diskConfig,
                      req,
                      landingZoneResources.region,
                      workspaceId,
                      ctx.now
                    )
                  )
                  _ <- authProvider
                    .notifyResourceCreatedV2(samResource, userInfo.userEmail, cloudContext, workspaceId, userInfo)
                    .handleErrorWith { t =>
                      log.error(t)(
                        s"[${ctx.traceId}] Failed to notify the AuthProvider for creation of persistent disk ${req.azureDiskConfig.name.value}"
                      ) >> F.raiseError(t)
                    }
                  disk <- persistentDiskQuery.save(pd).transaction
                } yield disk.id
            }

            runtime = convertToRuntime(
              workspaceId,
              runtimeName,
              cloudContext,
              userInfo,
              req,
              RuntimeSamResourceId(vmSamResourceId.toString),
              Set(runtimeImage, listenerImage, welderImage),
              Set.empty,
              ctx.now
            )

            runtimeConfig = RuntimeConfig.AzureConfig(
              MachineTypeName(req.machineSize.toString),
              Some(diskId),
              landingZoneResources.region
            )
            runtimeToSave = SaveCluster(cluster = runtime, runtimeConfig = runtimeConfig, now = ctx.now)
            savedRuntime <- clusterQuery.save(runtimeToSave).transaction
            _ <- publisherQueue.offer(
              CreateAzureRuntimeMessage(
                savedRuntime.id,
                workspaceId,
                storageContainer.resourceId,
                landingZoneResources,
                useExistingDisk,
                Some(ctx.traceId),
                workspaceDesc.displayName,
                storageContainer.name
              )
            )
          } yield ()
      }

    } yield CreateRuntimeResponse(ctx.traceId)

  override def getRuntime(userInfo: UserInfo, runtimeName: RuntimeName, workspaceId: WorkspaceId)(implicit
    as: Ask[F, AppContext]
  ): F[GetRuntimeResponse] =
    for {
      ctx <- as.ask

      hasWorkspacePermission <- authProvider.isUserWorkspaceReader(
        WorkspaceResourceSamResourceId(workspaceId),
        userInfo
      )
      _ <- F.raiseUnless(hasWorkspacePermission)(ForbiddenError(userInfo.userEmail))

      runtime <- RuntimeServiceDbQueries.getRuntimeByWorkspaceId(workspaceId, runtimeName).transaction

      hasPermission <-
        if (runtime.auditInfo.creator == userInfo.userEmail)
          F.pure(true)
        else
          checkSamPermission(
            WsmResourceSamResourceId(WsmControlledResourceId(UUID.fromString(runtime.samResource.resourceId))),
            userInfo,
            WsmResourceAction.Read
          ).map(_._1)

      _ <- ctx.span.traverse(s => F.delay(s.addAnnotation("Done auth call for get azure runtime permission")))
      _ <- F
        .raiseError[Unit](
          RuntimeNotFoundException(runtime.cloudContext, runtimeName, "permission denied", Some(ctx.traceId))
        )
        .whenA(!hasPermission)

    } yield runtime

  override def updateRuntime(userInfo: UserInfo,
                             runtimeName: RuntimeName,
                             workspaceId: WorkspaceId,
                             req: UpdateAzureRuntimeRequest
  )(implicit as: Ask[F, AppContext]): F[Unit] =
    F.pure(AzureUnimplementedException("patch not implemented yet"))

  override def deleteRuntime(userInfo: UserInfo,
                             runtimeName: RuntimeName,
                             workspaceId: WorkspaceId,
                             deleteDisk: Boolean
  )(implicit
    as: Ask[F, AppContext]
  ): F[Unit] =
    for {
      ctx <- as.ask

      hasWorkspacePermission <- authProvider.isUserWorkspaceReader(
        WorkspaceResourceSamResourceId(workspaceId),
        userInfo
      )
      _ <- F.raiseUnless(hasWorkspacePermission)(ForbiddenError(userInfo.userEmail))

      runtime <- RuntimeServiceDbQueries.getActiveRuntimeRecord(workspaceId, runtimeName).transaction

      _ <- F
        .raiseUnless(runtime.status.isDeletable)(
          RuntimeCannotBeDeletedException(runtime.cloudContext, runtime.runtimeName, runtime.status)
        )

      diskIdOpt <- RuntimeConfigQueries.getDiskId(runtime.runtimeConfigId).transaction
      diskId <- diskIdOpt match {
        case Some(value) => F.pure(value)
        case _ =>
          F.raiseError[DiskId](
            AzureRuntimeHasInvalidRuntimeConfig(runtime.cloudContext, runtime.runtimeName, ctx.traceId)
          )
      }

      // get wsm api
      wsmAzureResourceApi <- wsmClientProvider.getControlledAzureResourceApi(userInfo.accessToken.token)
      wsmResourceId = WsmControlledResourceId(UUID.fromString(runtime.internalId))

      // if the vm is found in WSM and has a deletable state,
      // then the resourceId is passed to back leo to make the delete call to WSM
      // (state can be BROKEN, CREATING, DELETING, READY, UPDATING or NULL)
      deletableStatus = List("BROKEN", "READY")

      attempt <- F.delay(wsmAzureResourceApi.getAzureVm(workspaceId.value, wsmResourceId.value)).attempt
      wsmVMResourceSamId <- attempt match {
        case Right(result) =>
          val vmState = result.getMetadata.getState.getValue
          val res = if (deletableStatus.contains(vmState)) Some(wsmResourceId) else None
          log
            .info(ctx.loggingCtx)(
              s"Runtime ${runtimeName.asString} with resourceId ${wsmResourceId.value} has a state of $vmState in WSM"
            )
            .as(res)
        case Left(e) =>
          log
            .info(ctx.loggingCtx)(
              s"No wsm record found for runtime ${runtimeName.asString} No-op for wsmDao.deleteVm, ${e.getMessage}"
            )
            .as(None)
      }

      hasPermission <-
        if (runtime.auditInfo.creator == userInfo.userEmail) F.pure(true)
        else
          authProvider
            .isUserWorkspaceOwner(WorkspaceResourceSamResourceId(workspaceId), userInfo)

      _ <- ctx.span.traverse(s => F.delay(s.addAnnotation("Done auth call for delete azure runtime permission")))
      _ <- F
        .raiseError[Unit](RuntimeNotFoundException(runtime.cloudContext, runtimeName, "permission denied"))
        .whenA(!hasPermission)

      // Query WSM for Landing Zone resources
      userToken = org.http4s.headers.Authorization(
        org.http4s.Credentials.Token(AuthScheme.Bearer, userInfo.accessToken.token)
      )
      workspaceDescOpt <- wsmDao.getWorkspace(workspaceId, userToken)
      workspaceDesc <- F.fromOption(workspaceDescOpt, WorkspaceNotFoundException(workspaceId, ctx.traceId))
      leoAuth <- samDAO.getLeoAuthToken
      landingZoneResources <- wsmDao.getLandingZoneResources(workspaceDesc.spendProfile, leoAuth)

      // Update DB record to Deleting status
      _ <- clusterQuery.markPendingDeletion(runtime.id, ctx.now).transaction

      // pass the disk to delete to publisher if specified
      diskIdToDelete <-
        if (deleteDisk)
          persistentDiskQuery.markPendingDeletion(diskId, ctx.now).transaction.as(diskIdOpt)
        else F.pure(none[DiskId])

      _ <- publisherQueue.offer(
        DeleteAzureRuntimeMessage(runtime.id,
                                  diskIdToDelete,
                                  workspaceId,
                                  wsmVMResourceSamId,
                                  landingZoneResources,
                                  Some(ctx.traceId)
        )
      )
    } yield ()

  override def deleteAllRuntimes(userInfo: UserInfo, workspaceId: WorkspaceId, deleteDisk: Boolean)(implicit
    as: Ask[F, AppContext]
  ): F[Unit] =
    for {
      ctx <- as.ask
      hasWorkspacePermission <- authProvider.isUserWorkspaceReader(
        WorkspaceResourceSamResourceId(workspaceId),
        userInfo
      )
      _ <- F.raiseUnless(hasWorkspacePermission)(ForbiddenError(userInfo.userEmail))
      // We should not list runtimes that are already in a `Deleted` status
      runtimes <- RuntimeServiceDbQueries
        .listRuntimesForWorkspace(Map.empty, List(RuntimeStatus.Deleted), None, Some(workspaceId), None)
        .map(_.toList)
        .transaction

      nonDeletableRuntimes = runtimes.filterNot(r => r.status.isDeletable)

      _ <-
        if (nonDeletableRuntimes.isEmpty)
          runtimes
            .map(r => r.clusterName)
            .traverse(runtime_name => deleteRuntime(userInfo, runtime_name, workspaceId, deleteDisk))
        else
          // Error out if any runtime is in a non deletable state
          F.raiseError[Unit](
            NonDeletableRuntimesInWorkspaceFoundException(workspaceId,
                                                          s"${nonDeletableRuntimes.map(r => r.clusterName)}"
            )
          )
    } yield ()

  override def updateDateAccessed(userInfo: UserInfo, workspaceId: WorkspaceId, runtimeName: RuntimeName)(implicit
    as: Ask[F, AppContext]
  ): F[Unit] =
    for {
      ctx <- as.ask
      hasWorkspacePermission <- authProvider.isUserWorkspaceReader(
        WorkspaceResourceSamResourceId(workspaceId),
        userInfo
      )
      _ <- F.raiseUnless(hasWorkspacePermission)(ForbiddenError(userInfo.userEmail))

      runtime <- RuntimeServiceDbQueries.getRuntimeByWorkspaceId(workspaceId, runtimeName).transaction

      hasResourcePermission <- checkSamPermission(
        WsmResourceSamResourceId(WsmControlledResourceId(UUID.fromString(runtime.samResource.resourceId))),
        userInfo,
        WsmResourceAction.Write
      ).map(_._1)

      _ <- ctx.span.traverse(s =>
        F.delay(s.addAnnotation("Done auth call for update date accessed runtime permission"))
      )
      _ <- F
        .raiseError[Unit](
          RuntimeNotFoundException(runtime.cloudContext, runtimeName, "permission denied", Some(ctx.traceId))
        )
        .whenA(!hasResourcePermission)

      _ <- dateAccessUpdaterQueue.offer(UpdateDateAccessMessage(runtimeName, runtime.cloudContext, ctx.now)) >>
        log.info(s"Queued message to update dateAccessed for runtime ${runtime.cloudContext}/$runtimeName")
    } yield ()

  def startRuntime(userInfo: UserInfo, runtimeName: RuntimeName, workspaceId: WorkspaceId)(implicit
    as: Ask[F, AppContext]
  ): F[Unit] = for {
    ctx <- as.ask
    hasWorkspacePermission <- authProvider.isUserWorkspaceReader(
      WorkspaceResourceSamResourceId(workspaceId),
      userInfo
    )
    _ <- F.raiseUnless(hasWorkspacePermission)(ForbiddenError(userInfo.userEmail))

    runtime <- RuntimeServiceDbQueries.getActiveRuntimeRecord(workspaceId, runtimeName).transaction

    hasResourcePermission <- checkPermission(
      runtime.auditInfo.creator,
      userInfo,
      WsmResourceSamResourceId(WsmControlledResourceId(UUID.fromString(runtime.internalId)))
    )

    _ <- F
      .raiseError[Unit](
        RuntimeNotFoundException(runtime.cloudContext, runtimeName, "permission denied", Some(ctx.traceId))
      )
      .whenA(!hasResourcePermission)
    _ <-
      if (runtime.status.isStartable) F.unit
      else
        F.raiseError[Unit](RuntimeCannotBeStartedException(runtime.cloudContext, runtime.runtimeName, runtime.status))
    _ <- clusterQuery.updateClusterStatus(runtime.id, RuntimeStatus.PreStarting, ctx.now).transaction
    _ <- publisherQueue.offer(StartRuntimeMessage(runtime.id, Some(ctx.traceId)))
  } yield ()

  def stopRuntime(userInfo: UserInfo, runtimeName: RuntimeName, workspaceId: WorkspaceId)(implicit
    as: Ask[F, AppContext]
  ): F[Unit] = for {
    ctx <- as.ask

    hasWorkspacePermission <- authProvider.isUserWorkspaceReader(
      WorkspaceResourceSamResourceId(workspaceId),
      userInfo
    )
    _ <- F.raiseUnless(hasWorkspacePermission)(ForbiddenError(userInfo.userEmail))

    runtime <- RuntimeServiceDbQueries.getActiveRuntimeRecord(workspaceId, runtimeName).transaction

    hasResourcePermission <- checkPermission(
      runtime.auditInfo.creator,
      userInfo,
      WsmResourceSamResourceId(WsmControlledResourceId(UUID.fromString(runtime.internalId)))
    )

    _ <- F
      .raiseError[Unit](
        RuntimeNotFoundException(runtime.cloudContext, runtimeName, "permission denied", Some(ctx.traceId))
      )
      .whenA(!hasResourcePermission)
    _ <-
      if (runtime.status.isStoppable) F.unit
      else
        F.raiseError[Unit](RuntimeCannotBeStoppedException(runtime.cloudContext, runtime.runtimeName, runtime.status))
    _ <- clusterQuery.updateClusterStatus(runtime.id, RuntimeStatus.PreStopping, ctx.now).transaction
    _ <- publisherQueue.offer(StopRuntimeMessage(runtime.id, Some(ctx.traceId)))
  } yield ()

  override def listRuntimes(
    userInfo: UserInfo,
    workspaceId: Option[WorkspaceId],
    cloudProvider: Option[CloudProvider],
    params: Map[String, String]
  )(implicit as: Ask[F, AppContext]): F[Vector[ListRuntimeResponse2]] =
    for {
      ctx <- as.ask

      // Authorize: user has an active account and has accepted terms of service
      _ <- authProvider.checkUserEnabled(userInfo)

      // Authorize: get resource IDs the user can see
      readerRuntimeIds: List[String] <- authProvider
        .getAuthorizedIds[RuntimeSamResourceId](isOwner = false, userInfo)
        .flatMap(ids => F.pure(ids.map(_.resourceId)))
      readerWorkspaceIds: List[String] <- authProvider
        .getAuthorizedIds[WorkspaceResourceSamResourceId](isOwner = false, userInfo)
        .flatMap(ids => F.pure(ids.map(_.resourceId)))
      ownerWorkspaceIds: List[String] <- authProvider
        .getAuthorizedIds[WorkspaceResourceSamResourceId](isOwner = true, userInfo)
        .flatMap(ids => F.pure(ids.map(_.resourceId)))
      readerProjectIds: List[String] <- authProvider
        .getAuthorizedIds[ProjectSamResourceId](isOwner = false, userInfo)
        .flatMap(ids => F.pure(ids.map(_.resourceId)))
      ownerProjectIds: List[String] <- authProvider
        .getAuthorizedIds[ProjectSamResourceId](isOwner = true, userInfo)
        .flatMap(ids => F.pure(ids.map(_.resourceId)))

      // Parameters: parse search filters from request
      (labelMap, includeDeleted, _) <- F.fromEither(processListParameters(params))
      excludeStatuses = if (includeDeleted) List.empty else List(RuntimeStatus.Deleted)
      creatorOnly <- F.fromEither(processCreatorOnlyParameter(userInfo.userEmail, params, ctx.traceId))

      runtimes <- RuntimeServiceDbQueries
        .listAuthorizedRuntimes(
          labelMap, // arbitrary key-value labels to filter by
          excludeStatuses, // whether to filter out Deleted runtimes
          creatorOnly, // whether to filter out runtimes user did not create
          workspaceId, // whether to find only runtimes in a single workspace
          cloudProvider, // Google | Azure
          // Authorization scopes
          readerRuntimeIds,
          readerWorkspaceIds,
          ownerWorkspaceIds,
          readerProjectIds,
          ownerProjectIds
        )
        .map(_.toList)
        .transaction

    } yield runtimes.toVector

  private[service] def convertToDisk(userInfo: UserInfo,
                                     cloudContext: CloudContext,
                                     diskName: DiskName,
                                     samResource: PersistentDiskSamResourceId,
                                     config: PersistentDiskConfig,
                                     req: CreateAzureRuntimeRequest,
                                     region: com.azure.core.management.Region,
                                     workspaceId: WorkspaceId,
                                     now: Instant
  ): Either[Throwable, PersistentDisk] = {
    // create a LabelMap of default labels
    val defaultLabelMap: LabelMap =
      Map(
        "diskName" -> diskName.value,
        "cloudContext" -> cloudContext.asString,
        "creator" -> userInfo.userEmail.value
      )

    // combine default and given labels
    val allLabels = req.azureDiskConfig.labels ++ defaultLabelMap

    for {
      // check the labels do not contain forbidden keys
      labels <-
        if (allLabels.contains(includeDeletedKey))
          Left(IllegalLabelKeyException(includeDeletedKey))
        else
          Right(allLabels)
    } yield PersistentDisk(
      DiskId(0),
      cloudContext,
      ZoneName(region.toString),
      diskName,
      userInfo.userEmail,
      samResource,
      DiskStatus.Creating,
      AuditInfo(userInfo.userEmail, now, None, now),
      req.azureDiskConfig.size.getOrElse(config.defaultDiskSizeGb),
      req.azureDiskConfig.diskType.getOrElse(config.defaultDiskType),
      config.defaultBlockSizeBytes,
      None,
      None,
      labels,
      None,
      None,
      Some(workspaceId)
    )
  }

  private def checkPermission(creator: WorkbenchEmail,
                              userInfo: UserInfo,
                              wsmResourceSamResourceId: WsmResourceSamResourceId
  )(implicit
    ev: Ask[F, AppContext]
  ) = if (creator == userInfo.userEmail) F.pure(true)
  else {
    for {
      ctx <- ev.ask
      res <- checkSamPermission(wsmResourceSamResourceId, userInfo, WsmResourceAction.Read).map(_._1)
      _ <- ctx.span.traverse(s => F.delay(s.addAnnotation("Done auth call for azure runtime permission check")))
    } yield res
  }

  private def checkSamPermission(wsmResourceSamResourceId: WsmResourceSamResourceId,
                                 userInfo: UserInfo,
                                 wsmResourceAction: WsmResourceAction
  )(implicit
    ctx: Ask[F, AppContext]
  ): F[(Boolean, WsmControlledResourceId)] =
    for {
      // TODO: generalize for google
      res <- authProvider.hasPermission(
        wsmResourceSamResourceId,
        wsmResourceAction,
        userInfo
      )
    } yield (res, wsmResourceSamResourceId.controlledResourceId)

  private def errorHandler(runtimeId: Long, ctx: AppContext): Throwable => F[Unit] =
    e =>
      clusterErrorQuery
        .save(runtimeId, RuntimeError(e.getMessage, None, ctx.now, Some(ctx.traceId)))
        .transaction >>
        clusterQuery.updateClusterStatus(runtimeId, RuntimeStatus.Error, ctx.now).transaction.void

  private def convertToRuntime(workspaceId: WorkspaceId,
                               runtimeName: RuntimeName,
                               cloudContext: CloudContext,
                               userInfo: UserInfo,
                               request: CreateAzureRuntimeRequest,
                               samResourceId: RuntimeSamResourceId,
                               runtimeImages: Set[RuntimeImage],
                               scopes: Set[String],
                               now: Instant
  ): Runtime = {
    // create a LabelMap of default labels
    val defaultLabels = DefaultRuntimeLabels(
      runtimeName,
      None,
      cloudContext,
      userInfo.userEmail,
      // TODO: use an azure service account
      Some(userInfo.userEmail),
      None,
      None,
      // TODO: Will need to be updated when we support RStudio on Azure or JupyterLab on GCP V2 endpoint
      Some(Tool.JupyterLab)
    ).toMap

    val allLabels = request.labels ++ defaultLabels

    Runtime(
      0,
      Some(workspaceId),
      samResource = samResourceId,
      runtimeName = runtimeName,
      cloudContext = cloudContext,
      // TODO: use an azure service account
      serviceAccount = userInfo.userEmail,
      asyncRuntimeFields = None,
      auditInfo = AuditInfo(userInfo.userEmail, now, None, now),
      kernelFoundBusyDate = None,
      proxyUrl = Runtime.getProxyUrl(config.proxyUrlBase, cloudContext, runtimeName, runtimeImages, None, allLabels),
      status = RuntimeStatus.PreCreating,
      labels = allLabels,
      userScriptUri = None,
      startUserScriptUri = None,
      errors = List.empty,
      userJupyterExtensionConfig = None,
      autopauseThreshold =
        request.autopauseThreshold.getOrElse(0), // TODO: default to 30 once we start supporting autopause
      defaultClientId = None,
      allowStop = false,
      runtimeImages = runtimeImages,
      scopes = scopes,
      welderEnabled = true,
      customEnvironmentVariables = request.customEnvironmentVariables,
      runtimeConfigId = RuntimeConfigId(-1),
      patchInProgress = false
    )
  }

}

final case class WorkspaceNotFoundException(workspaceId: WorkspaceId, traceId: TraceId)
    extends LeoException(
      s"WorkspaceId not found in workspace manager for workspace ${workspaceId}",
      StatusCodes.NotFound,
      traceId = Some(traceId)
    )

final case class CloudContextNotFoundException(workspaceId: WorkspaceId, traceId: TraceId)
    extends LeoException(
      s"Cloud context not found in workspace manager for workspace ${workspaceId}",
      StatusCodes.NotFound,
      traceId = Some(traceId)
    )

final case class AzureRuntimeControlledResourceNotFoundException(cloudContext: CloudContext,
                                                                 runtimeName: RuntimeName,
                                                                 traceId: TraceId
) extends LeoException(
      s"Controlled resource record not found for runtime ${cloudContext.asStringWithProvider}/${runtimeName.asString}",
      StatusCodes.NotFound,
      traceId = Some(traceId)
    )

final case class AzureRuntimeHasInvalidRuntimeConfig(cloudContext: CloudContext,
                                                     runtimeName: RuntimeName,
                                                     traceId: TraceId
) extends LeoException(
      s"Azure runtime ${cloudContext.asStringWithProvider}/${runtimeName.asString} was found with an invalid runtime config",
      StatusCodes.InternalServerError,
      traceId = Some(traceId)
    )

case class MultiplePersistentDisksException(workspaceId: WorkspaceId, numDisks: Int, disks: List[PersistentDisk])
    extends LeoException(
      s"Workspace: ${workspaceId.value} contains ${numDisks} persistent disks, must have only 1. Current PDs: ${disks
          .map(disks => s"(${disks.name.value},${disks.id.value})")}. Runtime cannot be created with an existing disk ",
      StatusCodes.PreconditionFailed,
      traceId = None
    )

case class NoPersistentDiskException(workspaceId: WorkspaceId)
    extends LeoException(
      s"Workspace: ${workspaceId.value} does not contain any persistent disks. Runtime cannot be created with an existing disk",
      StatusCodes.PreconditionFailed,
      traceId = None
    )

case class PersistentDiskNotReadyException(diskId: DiskId, diskStatus: DiskStatus)
    extends LeoException(
      s"Existing disk: ${diskId.value} has status ${diskStatus}. Runtime cannot be created with an existing disk",
      StatusCodes.PreconditionFailed,
      traceId = None
    )

case class OnlyOneRuntimePerWorkspacePerCreator(workspaceId: WorkspaceId,
                                                creator: WorkbenchEmail,
                                                runtime: RuntimeName,
                                                status: RuntimeStatus
) extends LeoException(
      s"There is already an active runtime ${runtime.asString} in this workspace ${workspaceId.value} created by user ${creator.value} with the status ${status}. New runtime cannot be created until this one is deleted",
      StatusCodes.PreconditionFailed,
      traceId = None
    )
