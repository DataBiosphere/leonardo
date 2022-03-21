package org.broadinstitute.dsde.workbench.leonardo.http
package service

import java.time.Instant
import akka.http.scaladsl.model.StatusCodes

import scala.concurrent.ExecutionContext
import org.broadinstitute.dsde.workbench.leonardo.config.PersistentDiskConfig
import cats.effect.Async
import cats.implicits._
import org.broadinstitute.dsde.workbench.leonardo.db.{
  clusterQuery,
  controlledResourceQuery,
  persistentDiskQuery,
  DbReference,
  RuntimeServiceDbQueries,
  SaveCluster,
  WsmResourceType
}
import org.broadinstitute.dsde.workbench.leonardo.model.{
  ForbiddenError,
  IllegalLabelKeyException,
  LeoAuthProvider,
  LeoException,
  RuntimeAlreadyExistsException,
  RuntimeCannotBeDeletedException,
  RuntimeNotFoundException
}
import cats.effect.std.Queue
import cats.Parallel
import cats.mtl.Ask
import org.broadinstitute.dsde.workbench.google2.{DiskName, MachineTypeName, ZoneName}
import org.broadinstitute.dsde.workbench.leonardo.SamResourceId.{
  PersistentDiskSamResourceId,
  RuntimeSamResourceId,
  WorkspaceResourceSamResourceId,
  WsmResourceSamResourceId
}
import org.broadinstitute.dsde.workbench.leonardo.dao.{SamDAO, WsmDao}
import org.broadinstitute.dsde.workbench.leonardo.{
  AppContext,
  AuditInfo,
  AzureUnimplementedException,
  CloudContext,
  CreateAzureRuntimeRequest,
  DefaultRuntimeLabels,
  DiskId,
  DiskStatus,
  LabelMap,
  PersistentDisk,
  Runtime,
  RuntimeConfig,
  RuntimeConfigId,
  RuntimeImage,
  RuntimeImageType,
  RuntimeName,
  RuntimeStatus,
  UpdateAzureRuntimeRequest,
  WorkspaceAction,
  WorkspaceId,
  WsmResourceAction
}
import org.broadinstitute.dsde.workbench.leonardo.monitor.LeoPubsubMessage
import org.broadinstitute.dsde.workbench.leonardo.monitor.LeoPubsubMessage.{
  CreateAzureRuntimeMessage,
  DeleteAzureRuntimeMessage
}
import org.broadinstitute.dsde.workbench.model.{TraceId, UserInfo}

class AzureServiceInterp[F[_]: Parallel](config: RuntimeServiceConfig,
                                         authProvider: LeoAuthProvider[F],
                                         wsmDao: WsmDao[F],
                                         samDAO: SamDAO[F],
                                         publisherQueue: Queue[F, LeoPubsubMessage])(
  implicit F: Async[F],
  dbReference: DbReference[F],
  ec: ExecutionContext
) extends AzureService[F] {
  override def createRuntime(userInfo: UserInfo,
                             runtimeName: RuntimeName,
                             workspaceId: WorkspaceId,
                             req: CreateAzureRuntimeRequest)(implicit as: Ask[F, AppContext]): F[Unit] =
    for {
      context <- as.ask

      leoAuth <- samDAO.getLeoAuthToken

      azureContext <- wsmDao.getWorkspace(workspaceId, leoAuth).map(_.azureContext)
      cloudContext = CloudContext.Azure(azureContext)

      samResource = WorkspaceResourceSamResourceId(workspaceId.value.toString)

      hasPermission <- authProvider.hasPermission(samResource, WorkspaceAction.CreateControlledUserResource, userInfo)

      _ <- context.span.traverse(s => F.delay(s.addAnnotation("Done auth call for azure runtime permission")))
      _ <- F
        .raiseError[Unit](ForbiddenError(userInfo.userEmail))
        .whenA(!hasPermission)

      runtimeOpt <- RuntimeServiceDbQueries.getStatusByName(cloudContext, runtimeName).transaction
      _ <- context.span.traverse(s => F.delay(s.addAnnotation("Done DB query for azure runtime")))

      runtimeImage: RuntimeImage = RuntimeImage(
        RuntimeImageType.Azure,
        req.imageUri.map(_.value).getOrElse(config.azureConfig.runtimeConfig.imageUri.value),
        None,
        context.now
      )

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
                context.now
              )
            )

            runtime = convertToRuntime(runtimeName,
                                       cloudContext,
                                       userInfo,
                                       req,
                                       RuntimeSamResourceId(samResource.resourceId),
                                       Set(runtimeImage),
                                       context.now)

            disk <- persistentDiskQuery.save(diskToSave).transaction

            runtimeToSave = SaveCluster(
              cluster = runtime,
              runtimeConfig = RuntimeConfig.AzureConfig(
                MachineTypeName(req.machineSize.toString),
                disk.id,
                req.region
              ),
              now = context.now,
              workspaceId = Some(workspaceId)
            )
            savedRuntime <- clusterQuery.save(runtimeToSave).transaction
            _ <- publisherQueue.offer(
              CreateAzureRuntimeMessage(savedRuntime.id, workspaceId, runtimeImage, Some(context.traceId))
            )
          } yield ()
      }

    } yield ()

  override def getRuntime(userInfo: UserInfo, runtimeName: RuntimeName, workspaceId: WorkspaceId)(
    implicit as: Ask[F, AppContext]
  ): F[GetRuntimeResponse] =
    for {
      leoAuth <- samDAO.getLeoAuthToken
      context <- as.ask
      azureContext <- wsmDao.getWorkspace(workspaceId, leoAuth).map(_.azureContext)
      cloudContext = CloudContext.Azure(azureContext)

      runtime <- RuntimeServiceDbQueries.getRuntime(cloudContext, runtimeName).transaction

      controlledResourceOpt <- controlledResourceQuery
        .getResourceTypeForRuntime(runtime.id, WsmResourceType.AzureVm)
        .transaction
      azureRuntimeControlledResource <- F.fromOption(
        controlledResourceOpt,
        AzureRuntimeControlledResourceNotFoundException(cloudContext, runtimeName, context.traceId)
      )

      hasPermission <- authProvider.hasPermission(WsmResourceSamResourceId(azureRuntimeControlledResource.resourceId),
                                                  WsmResourceAction.Read,
                                                  userInfo)

      _ <- context.span.traverse(s => F.delay(s.addAnnotation("Done auth call for get azure runtime permission")))
      _ <- F
        .raiseError[Unit](
          RuntimeNotFoundException(cloudContext, runtimeName, "permission denied", Some(context.traceId))
        )
        .whenA(!hasPermission)

    } yield runtime

  override def updateRuntime(userInfo: UserInfo,
                             runtimeName: RuntimeName,
                             workspaceId: WorkspaceId,
                             req: UpdateAzureRuntimeRequest)(implicit as: Ask[F, AppContext]): F[Unit] =
    F.pure(AzureUnimplementedException("patch not implemented yet"))

  override def deleteRuntime(userInfo: UserInfo, runtimeName: RuntimeName, workspaceId: WorkspaceId)(
    implicit as: Ask[F, AppContext]
  ): F[Unit] =
    for {
      leoAuth <- samDAO.getLeoAuthToken
      context <- as.ask
      azureContext <- wsmDao.getWorkspace(workspaceId, leoAuth).map(_.azureContext)
      cloudContext = CloudContext.Azure(azureContext)

      runtime <- RuntimeServiceDbQueries.getRuntime(cloudContext, runtimeName).transaction
      diskId <- runtime.runtimeConfig match {
        case config: RuntimeConfig.AzureConfig => F.pure(config.persistentDiskId)
        case _ =>
          F.raiseError[DiskId](
            AzureRuntimeHasInvalidRuntimeConfig(cloudContext, runtime.clusterName, context.traceId)
          )
      }

      controlledResourceOpt <- controlledResourceQuery
        .getResourceTypeForRuntime(runtime.id, WsmResourceType.AzureVm)
        .transaction
      azureRuntimeControlledResource <- F.fromOption(
        controlledResourceOpt,
        AzureRuntimeControlledResourceNotFoundException(cloudContext, runtimeName, context.traceId)
      )

      hasPermission <- authProvider.hasPermission(WsmResourceSamResourceId(azureRuntimeControlledResource.resourceId),
                                                  WsmResourceAction.Write,
                                                  userInfo)

      _ <- context.span.traverse(s => F.delay(s.addAnnotation("Done auth call for delete azure runtime permission")))
      _ <- F
        .raiseError[Unit](RuntimeNotFoundException(cloudContext, runtimeName, "permission denied"))
        .whenA(!hasPermission)

      _ <- F
        .raiseError[Unit](RuntimeCannotBeDeletedException(cloudContext, runtime.clusterName, runtime.status))
        .whenA(!runtime.status.isDeletable)

      _ <- clusterQuery.markPendingDeletion(runtime.id, context.now).transaction

      // For now, azure disk life cycle is tied to vm life cycle and incompatible with disk routes
      _ <- persistentDiskQuery.markPendingDeletion(diskId, context.now).transaction

      _ <- publisherQueue.offer(
        DeleteAzureRuntimeMessage(runtime.id,
                                  Some(diskId),
                                  workspaceId,
                                  azureRuntimeControlledResource.resourceId,
                                  Some(context.traceId))
      )
    } yield ()

  private[service] def convertToDisk(userInfo: UserInfo,
                                     cloudContext: CloudContext,
                                     diskName: DiskName,
                                     config: PersistentDiskConfig,
                                     req: CreateAzureRuntimeRequest,
                                     now: Instant): Either[Throwable, PersistentDisk] = {
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
      labels <- if (allLabels.contains(includeDeletedKey))
        Left(IllegalLabelKeyException(includeDeletedKey))
      else
        Right(allLabels)
    } yield PersistentDisk(
      DiskId(0),
      cloudContext,
      ZoneName(req.region.toString),
      diskName,
      userInfo.userEmail,
      //TODO: WSM will populate this, we can update in backleo if its needed for anything
      PersistentDiskSamResourceId("fakeUUID"),
      DiskStatus.Creating,
      AuditInfo(userInfo.userEmail, now, None, now),
      req.azureDiskConfig.size.getOrElse(config.defaultDiskSizeGb),
      req.azureDiskConfig.diskType.getOrElse(config.defaultDiskType),
      config.defaultBlockSizeBytes,
      None,
      None,
      labels
    )
  }

  private def convertToRuntime(runtimeName: RuntimeName,
                               cloudContext: CloudContext,
                               userInfo: UserInfo,
                               request: CreateAzureRuntimeRequest,
                               samResourceId: RuntimeSamResourceId,
                               runtimeImages: Set[RuntimeImage],
                               now: Instant): Runtime = {
    // create a LabelMap of default labels
    val defaultLabels = DefaultRuntimeLabels(
      runtimeName,
      None,
      cloudContext,
      userInfo.userEmail,
      //TODO: use an azure service account
      Some(userInfo.userEmail),
      None,
      None,
      Some(RuntimeImageType.Azure)
    ).toMap

    val allLabels = request.labels ++ defaultLabels

    Runtime(
      0,
      samResource = samResourceId,
      runtimeName = runtimeName,
      cloudContext = cloudContext,
      //TODO: use an azure service account
      serviceAccount = userInfo.userEmail,
      asyncRuntimeFields = None,
      auditInfo = AuditInfo(userInfo.userEmail, now, None, now),
      kernelFoundBusyDate = None,
      proxyUrl = Runtime.getProxyUrl(config.proxyUrlBase, cloudContext, runtimeName, runtimeImages, allLabels),
      status = RuntimeStatus.PreCreating,
      labels = allLabels,
      userScriptUri = None,
      startUserScriptUri = None,
      errors = List.empty,
      userJupyterExtensionConfig = None,
      autopauseThreshold = 30,
      defaultClientId = None,
      allowStop = false,
      runtimeImages = runtimeImages,
      scopes = config.azureConfig.runtimeConfig.defaultScopes,
      welderEnabled = true,
      customEnvironmentVariables = request.customEnvironmentVariables,
      runtimeConfigId = RuntimeConfigId(-1),
      patchInProgress = false
    )
  }

}

final case class AzureRuntimeControlledResourceNotFoundException(cloudContext: CloudContext,
                                                                 runtimeName: RuntimeName,
                                                                 traceId: TraceId)
    extends LeoException(
      s"Controlled resource record not found for runtime ${cloudContext.asStringWithProvider}/${runtimeName.asString}",
      StatusCodes.NotFound,
      traceId = Some(traceId)
    )

final case class AzureRuntimeHasInvalidRuntimeConfig(cloudContext: CloudContext,
                                                     runtimeName: RuntimeName,
                                                     traceId: TraceId)
    extends LeoException(
      s"Azure runtime ${cloudContext.asStringWithProvider}/${runtimeName.asString} was found with an invalid runtime config",
      StatusCodes.InternalServerError,
      traceId = Some(traceId)
    )
