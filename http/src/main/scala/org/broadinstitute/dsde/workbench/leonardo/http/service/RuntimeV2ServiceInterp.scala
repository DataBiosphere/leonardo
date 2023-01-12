package org.broadinstitute.dsde.workbench.leonardo
package http
package service

import akka.http.scaladsl.model.StatusCodes
import cats.Parallel
import cats.data.NonEmptyList
import cats.effect.Async
import cats.effect.std.Queue
import cats.mtl.Ask
import cats.syntax.all._
import org.broadinstitute.dsde.workbench.google2.{DiskName, MachineTypeName, ZoneName}
import org.broadinstitute.dsde.workbench.leonardo.JsonCodec._
import org.broadinstitute.dsde.workbench.leonardo.SamResourceId.{
  PersistentDiskSamResourceId,
  RuntimeSamResourceId,
  WorkspaceResourceSamResourceId,
  WsmResourceSamResourceId
}
import org.broadinstitute.dsde.workbench.leonardo.config.PersistentDiskConfig
import org.broadinstitute.dsde.workbench.leonardo.dao._
import org.broadinstitute.dsde.workbench.leonardo.db._
import org.broadinstitute.dsde.workbench.leonardo.model.SamResourceAction.runtimeSamResourceAction
import org.broadinstitute.dsde.workbench.leonardo.model._
import org.broadinstitute.dsde.workbench.leonardo.monitor.LeoPubsubMessage
import org.broadinstitute.dsde.workbench.leonardo.monitor.LeoPubsubMessage.{
  CreateAzureRuntimeMessage,
  DeleteAzureRuntimeMessage,
  StartRuntimeMessage,
  StopRuntimeMessage
}
import org.broadinstitute.dsde.workbench.model.{TraceId, UserInfo, WorkbenchEmail}
import org.http4s.AuthScheme

import java.time.Instant
import java.util.UUID
import scala.concurrent.ExecutionContext

class RuntimeV2ServiceInterp[F[_]: Parallel](config: RuntimeServiceConfig,
                                             authProvider: LeoAuthProvider[F],
                                             wsmDao: WsmDao[F],
                                             samDAO: SamDAO[F],
                                             publisherQueue: Queue[F, LeoPubsubMessage]
)(implicit
  F: Async[F],
  dbReference: DbReference[F],
  ec: ExecutionContext
) extends RuntimeV2Service[F] {
  override def createRuntime(userInfo: UserInfo,
                             runtimeName: RuntimeName,
                             workspaceId: WorkspaceId,
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
        BadRequestException(s"Workspace ${workspaceId} doesn't have storage container provisioned appropriately",
                            Some(ctx.traceId)
        )
      )
      runtimeOpt <- RuntimeServiceDbQueries.getStatusByName(cloudContext, runtimeName).transaction
      _ <- ctx.span.traverse(s => F.delay(s.addAnnotation("Done DB query for azure runtime")))

      // Get the Landing Zone Resources for the app for Azure
      leoAuth <- samDAO.getLeoAuthToken
      landingZoneResources <- cloudContext.cloudProvider match {
        case CloudProvider.Gcp =>
          F.raiseError(
            BadRequestException(s"Workspace ${workspaceId} is GCP and doesn't support V2 VM creation",
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
            diskToSave <- F.fromEither(
              convertToDisk(
                userInfo,
                cloudContext,
                DiskName(req.azureDiskConfig.name.value),
                config.azureConfig.diskConfig,
                req,
                ctx.now
              )
            )

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

            disk <- persistentDiskQuery.save(diskToSave).transaction
            runtimeConfig = RuntimeConfig.AzureConfig(
              MachineTypeName(req.machineSize.toString),
              disk.id,
              req.region
            )
            runtimeToSave = SaveCluster(cluster = runtime, runtimeConfig = runtimeConfig, now = ctx.now)
            savedRuntime <- clusterQuery.save(runtimeToSave).transaction
            _ <- publisherQueue.offer(
              CreateAzureRuntimeMessage(savedRuntime.id,
                                        workspaceId,
                                        storageContainer.resourceId,
                                        landingZoneResources,
                                        Some(ctx.traceId)
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

      runtime <- RuntimeServiceDbQueries.getActiveRuntime(workspaceId, runtimeName).transaction

      // If user is creator of the runtime, they should definitely be able to see the runtime.
      hasPermission <-
        if (runtime.auditInfo.creator == userInfo.userEmail) F.pure(true)
        else {
          checkSamPermission(
            WsmResourceSamResourceId(WsmControlledResourceId(UUID.fromString(runtime.samResource.resourceId))),
            userInfo,
            WsmResourceAction.Read
          ).map(_._1)
        }

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

      // We only update IP to properly after the VM is created in WSM. Hence, if IP is not defined, we don't need to pass the VM resourceId
      // to back leo, where it'll trigger a WSM call to delete the VM
      wsmVMResourceSamId = runtime.hostIp.map(_ => WsmControlledResourceId(UUID.fromString(runtime.internalId)))
      // If there's no controlled resource record, that means we created the DB record in front leo but failed somewhere in back leo (possibly in polling WSM)
      // This is non-fatal, as we still want to allow users to clean up the db record if they have permission.
      // We must check if they have permission other ways if we did not get an ID back from WSM though
      hasPermission <-
        if (runtime.auditInfo.creator == userInfo.userEmail)
          F.pure(true)
        else
          authProvider
            .isUserWorkspaceOwner(WorkspaceResourceSamResourceId(workspaceId), userInfo)

      _ <- ctx.span.traverse(s => F.delay(s.addAnnotation("Done auth call for delete azure runtime permission")))
      _ <- F
        .raiseError[Unit](RuntimeNotFoundException(runtime.cloudContext, runtimeName, "permission denied"))
        .whenA(!hasPermission)

      _ <- clusterQuery.markPendingDeletion(runtime.id, ctx.now).transaction

      // pass the disk to delete to publisher if specified
      diskIdToDelete <-
        if (deleteDisk)
          persistentDiskQuery.markPendingDeletion(diskId, ctx.now).transaction.as(diskIdOpt)
        else F.pure(none[DiskId])

      _ <- publisherQueue.offer(
        DeleteAzureRuntimeMessage(runtime.id, diskIdToDelete, workspaceId, wsmVMResourceSamId, Some(ctx.traceId))
      )
    } yield ()

  def startRuntime(userInfo: UserInfo, runtimeName: RuntimeName, workspaceId: WorkspaceId)(implicit
    as: Ask[F, AppContext]
  ): F[Unit] = for {
    ctx <- as.ask
    runtime <- RuntimeServiceDbQueries.getActiveRuntimeRecord(workspaceId, runtimeName).transaction

    // If user is creator of the runtime, they should definitely be able to see the runtime.
    hasPermission <- checkPermission(
      runtime.auditInfo.creator,
      userInfo,
      WsmResourceSamResourceId(WsmControlledResourceId(UUID.fromString(runtime.internalId)))
    )
    _ <- F
      .raiseError[Unit](
        RuntimeNotFoundException(runtime.cloudContext, runtimeName, "permission denied", Some(ctx.traceId))
      )
      .whenA(!hasPermission)
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
    runtime <- RuntimeServiceDbQueries.getActiveRuntimeRecord(workspaceId, runtimeName).transaction

    // If user is creator of the runtime, they should definitely be able to see the runtime.
    hasPermission <- checkPermission(
      runtime.auditInfo.creator,
      userInfo,
      WsmResourceSamResourceId(WsmControlledResourceId(UUID.fromString(runtime.internalId)))
    )

    _ <- F
      .raiseError[Unit](
        RuntimeNotFoundException(runtime.cloudContext, runtimeName, "permission denied", Some(ctx.traceId))
      )
      .whenA(!hasPermission)
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
      (labelMap, includeDeleted, _) <- F.fromEither(processListParameters(params))

      creatorOnly <- F.fromEither(processCreatorOnlyParameter(userInfo.userEmail, params, ctx.traceId))
      runtimes <- RuntimeServiceDbQueries
        .listRuntimesForWorkspace(labelMap, includeDeleted, creatorOnly, workspaceId, cloudProvider)
        .map(_.toList)
        .transaction
      filteredRuntimes <-
        if (creatorOnly.isDefined) {
          F.pure(runtimes)
        } else {
          val (runtimesUserIsCreator, runtimesUserIsNotCreator) =
            runtimes.partition(_.auditInfo.creator == userInfo.userEmail)

          // Here, we optimize the SAM lookups based on whether we will need to use the google project fallback
          // IF (workspaceId) EXISTS we need WSM resource ID sam lookup + workspaceId Fallback
          // ELSE we need googleProject fallback
          val (runtimesUserIsNotCreatorWithWorkspaceId, runtimesUserIsNotCreatorWithoutWorkspaceId) =
            runtimesUserIsNotCreator
              .partition(_.workspaceId.isDefined)

          // Here, we check if backleo has updated the runtime sam id with the wsm resource's UUID via type conversion
          // If it has, we can then use the samResourceId of the runtime (which is the same as the wsm resource id) for permission lookup

          // -----------   Filter runtimes that's in runtimesUserIsNotCreatorWithWorkspaceId   -----------
          val runtimesUserIsNotCreatorWithWorkspaceSamIds = NonEmptyList
            .fromList(
              runtimesUserIsNotCreatorWithWorkspaceId.flatMap(runtime =>
                runtime.workspaceId match {
                  case Some(id) => List((runtime, WorkspaceResourceSamResourceId(id)))
                  case None     => List.empty
                }
              )
            )
          for {
            // If user is not creator for the runtime, then we check if they're workspace owner
            runtimesUserIsNotCreatorWithWorkspaceIdAndSamResourceId <- runtimesUserIsNotCreatorWithWorkspaceSamIds
              .traverse(workspaces =>
                authProvider.filterWorkspaceOwner(
                  workspaces.map(_._2),
                  userInfo
                )
              )
              .map(_.getOrElse(Set.empty))

            samUserVisibleRuntimesUserIsNotCreatorWithWorkspace =
              runtimesUserIsNotCreatorWithWorkspaceId.mapFilter { runtime =>
                if (
                  runtimesUserIsNotCreatorWithWorkspaceIdAndSamResourceId
                    .exists(workspaceSamId => workspaceSamId.workspaceId == runtime.workspaceId.get)
                ) {
                  Some(runtime)
                } else None
              }

            // -----------   Filter runtimes that's in runtimesUserIsNotCreatorWithoutWorkspaceId   -----------
            // We must also check the RuntimeSamResourceId in sam to support already existing and newly created google runtimes
            runtimesAndProjects = runtimesUserIsNotCreatorWithoutWorkspaceId
              .mapFilter { case rt =>
                rt.cloudContext match {
                  case CloudContext.Gcp(googleProject) =>
                    Some((rt, googleProject, rt.samResource))
                  case CloudContext.Azure(_) =>
                    None // This should never happen cuz all Azure runtimes has a workspaceId
                }
              }

            samProjectVisibleSamIds <- NonEmptyList.fromList(runtimesAndProjects.map(x => (x._2, x._3))).traverse {
              rs =>
                authProvider
                  .filterUserVisibleWithProjectFallback(
                    rs,
                    userInfo
                  )
            }

            samVisibleRuntimesWithoutWorkspaceId = samProjectVisibleSamIds match {
              case Some(projectsAndSamIds) =>
                runtimesAndProjects.mapFilter { case (rt, _, runtimeSamId) =>
                  if (projectsAndSamIds.map(_._2).contains(runtimeSamId))
                    Some(rt)
                  else None
                }
              case None => List.empty
            }
          } yield
          // runtimesUserIsCreator: runtimes user is creator
          // samUserVisibleRuntimesUserIsNotCreatorWithWorkspace: runtimes user is not creator, but it's visible by checking with Sam directly.
          //                                                      Note here we don't need to check workspace level permission because for WSM created resources, permissions
          //                                                      built hierarchically. So by asking Sam if user can read a runtime will implicitly check user's permission at workspace level
          // samVisibleRuntimesWithoutWorkspaceId: runtimes user is not creator, but user can view the runtime according to Sam
          runtimesUserIsCreator ++ samUserVisibleRuntimesUserIsNotCreatorWithWorkspace ++ samVisibleRuntimesWithoutWorkspaceId
        }

    } yield filteredRuntimes.toVector

  private[service] def convertToDisk(userInfo: UserInfo,
                                     cloudContext: CloudContext,
                                     diskName: DiskName,
                                     config: PersistentDiskConfig,
                                     req: CreateAzureRuntimeRequest,
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
      ZoneName(req.region.toString),
      diskName,
      userInfo.userEmail,
      // TODO: WSM will populate this, we can update in backleo if its needed for anything
      PersistentDiskSamResourceId("fakeUUID"),
      DiskStatus.Creating,
      AuditInfo(userInfo.userEmail, now, None, now),
      req.azureDiskConfig.size.getOrElse(config.defaultDiskSizeGb),
      req.azureDiskConfig.diskType.getOrElse(config.defaultDiskType),
      config.defaultBlockSizeBytes,
      None,
      None,
      labels,
      None
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
        request.autoPauseThreshold.getOrElse(0), // TODO: default to 30 once we start supporting autopause
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
