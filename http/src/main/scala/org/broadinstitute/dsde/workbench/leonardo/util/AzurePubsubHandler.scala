package org.broadinstitute.dsde.workbench
package leonardo
package util

import cats.Parallel
import cats.effect.Async
import cats.effect.std.Queue
import cats.mtl.Ask
import cats.syntax.all._
import com.azure.resourcemanager.compute.models.{VirtualMachine, VirtualMachineSizeTypes}
import org.broadinstitute.dsde.workbench.azure._
import org.broadinstitute.dsde.workbench.google2.{streamFUntilDone, streamUntilDoneOrTimeout}
import org.broadinstitute.dsde.workbench.leonardo.AsyncTaskProcessor.Task
import org.broadinstitute.dsde.workbench.leonardo.config.ContentSecurityPolicyConfig
import org.broadinstitute.dsde.workbench.leonardo.dao._
import org.broadinstitute.dsde.workbench.leonardo.db._
import org.broadinstitute.dsde.workbench.leonardo.http.{ctxConversion, dbioToIO}
import org.broadinstitute.dsde.workbench.leonardo.monitor.LeoPubsubMessage.{
  CreateAzureRuntimeMessage,
  DeleteAzureRuntimeMessage
}
import org.broadinstitute.dsde.workbench.leonardo.monitor.PubsubHandleMessageError
import org.broadinstitute.dsde.workbench.leonardo.monitor.PubsubHandleMessageError._
import org.broadinstitute.dsde.workbench.model.{IP, WorkbenchEmail}
import org.broadinstitute.dsde.workbench.util2.InstanceName
import org.http4s.headers.Authorization
import org.typelevel.log4cats.StructuredLogger

import java.time.{Duration, Instant}
import java.util.UUID
import scala.concurrent.ExecutionContext

class AzurePubsubHandlerInterp[F[_]: Parallel](
  config: AzurePubsubHandlerConfig,
  contentSecurityPolicyConfig: ContentSecurityPolicyConfig,
  asyncTasks: Queue[F, Task[F]],
  wsmDao: WsmDao[F],
  samDAO: SamDAO[F],
  welderDao: WelderDAO[F],
  jupyterDAO: JupyterDAO[F],
  azureRelay: AzureRelayService[F],
  azureVmServiceInterp: AzureVmService[F],
  aksAlgebra: AKSAlgebra[F]
)(implicit val executionContext: ExecutionContext, dbRef: DbReference[F], logger: StructuredLogger[F], F: Async[F])
    extends AzurePubsubHandlerAlgebra[F] {
  implicit val wsmDeleteVmDoneCheckable: DoneCheckable[Option[GetDeleteJobResult]] = (v: Option[GetDeleteJobResult]) =>
    v match {
      case Some(vv) =>
        vv.jobReport.status.equals(WsmJobStatus.Succeeded) || vv.jobReport.status == WsmJobStatus.Failed
      case None =>
        true
    }

  implicit val isJupyterUpDoneCheckable: DoneCheckable[Boolean] = (v: Boolean) => v

  override def createAndPollRuntime(msg: CreateAzureRuntimeMessage)(implicit ev: Ask[F, AppContext]): F[Unit] =
    for {
      ctx <- ev.ask
      runtimeOpt <- clusterQuery.getClusterById(msg.runtimeId).transaction
      runtime <- F.fromOption(runtimeOpt, PubsubHandleMessageError.ClusterNotFound(msg.runtimeId, msg))
      runtimeConfig <- RuntimeConfigQueries.getRuntimeConfig(runtime.runtimeConfigId).transaction
      azureConfig <- runtimeConfig match {
        case x: RuntimeConfig.AzureConfig => F.pure(x)
        case x => F.raiseError(new RuntimeException(s"this runtime doesn't have proper azure config ${x}"))
      }
      createVmJobId = WsmJobId(s"create-vm-${ctx.traceId.asString.take(10)}")
      _ <- createRuntime(
        CreateAzureRuntimeParams(
          msg.workspaceId,
          runtime,
          msg.storageContainerResourceId,
          msg.landingZoneResources,
          azureConfig,
          config.runtimeDefaults.image,
          msg.useExistingDisk,
          msg.workspaceName,
          msg.containerName
        ),
        WsmJobControl(createVmJobId)
      )
      _ <- monitorCreateRuntime(
        PollRuntimeParams(msg.workspaceId,
                          runtime,
                          createVmJobId,
                          msg.landingZoneResources.relayNamespace,
                          msg.useExistingDisk
        )
      )
    } yield ()

  /** Creates an Azure VM but doesn't wait for its completion.
   * This includes creation of all child Azure resources (disk, network, ip), and assumes these are created synchronously
   * If useExistingDisk is specified, disk is transferred to new runtime and no new disk is created
   * */
  private def createRuntime(params: CreateAzureRuntimeParams, jobControl: WsmJobControl)(implicit
    ev: Ask[F, AppContext]
  ): F[Unit] =
    for {
      ctx <- ev.ask
      auth <- samDAO.getLeoAuthToken

      cloudContext <- params.runtime.cloudContext match {
        case _: CloudContext.Gcp =>
          F.raiseError[AzureCloudContext](
            PubsubHandleMessageError.ClusterError(params.runtime.id,
                                                  ctx.traceId,
                                                  "Azure runtime should not have GCP cloud context"
            )
          )
        case x: CloudContext.Azure => F.pure(x.value)
      }
      hcName = RelayHybridConnectionName(params.runtime.runtimeName.asString)
      primaryKey <- azureRelay.createRelayHybridConnection(params.landingZoneResources.relayNamespace,
                                                           hcName,
                                                           cloudContext
      )

      createDiskAction = createDisk(params, auth)

      (stagingContainerName, stagingContainerResourceId) <- createStorageContainer(params, auth)

      samResourceId = WsmControlledResourceId(UUID.fromString(params.runtime.samResource.resourceId))
      wsStorageContainerUrl =
        s"https://${params.landingZoneResources.storageAccountName.value}.blob.core.windows.net/${params.storageContainerName.value}"
      createVmRequest <- createDiskAction.map { diskResp =>
        val vmCommon = getCommonFields(
          ControlledResourceName(params.runtime.runtimeName.asString),
          config.runtimeDefaults.vmControlledResourceDesc,
          params.runtime.auditInfo.creator,
          Some(samResourceId)
        )

        val arguments = List(
          params.landingZoneResources.relayNamespace.value,
          hcName.value,
          "localhost",
          primaryKey.value,
          config.runtimeDefaults.listenerImage,
          config.samUrl.renderString,
          samResourceId.value.toString,
          "csp.txt",
          config.wsmUrl.renderString,
          params.workspaceId.value.toString,
          params.storageContainerResourceId.value.toString,
          config.welderImage,
          params.runtime.auditInfo.creator.value,
          stagingContainerName.value,
          stagingContainerResourceId.value.toString,
          params.workspaceName,
          wsStorageContainerUrl
        )
        println("---------")
        println(config.runtimeDefaults.customScriptExtension.fileUris)
        println("---------")
        logger.info("+++++++++")
        logger.info(config.runtimeDefaults.customScriptExtension.fileUris.toString())
        val cmdToExecute =
          s"echo \"${contentSecurityPolicyConfig.asString}\" > csp.txt && bash azure_vm_init_script.sh ${arguments.mkString(" ")}"
        CreateVmRequest(
          params.workspaceId,
          vmCommon,
          CreateVmRequestData(
            params.runtime.runtimeName,
            params.runtimeConfig.region,
            VirtualMachineSizeTypes.fromString(params.runtimeConfig.machineType.value),
            config.runtimeDefaults.image,
            CustomScriptExtension(
              name = config.runtimeDefaults.customScriptExtension.name,
              publisher = config.runtimeDefaults.customScriptExtension.publisher,
              `type` = config.runtimeDefaults.customScriptExtension.`type`,
              version = config.runtimeDefaults.customScriptExtension.version,
              minorVersionAutoUpgrade = config.runtimeDefaults.customScriptExtension.minorVersionAutoUpgrade,
              protectedSettings = ProtectedSettings(
                config.runtimeDefaults.customScriptExtension.fileUris,
                cmdToExecute
              )
            ),
            config.runtimeDefaults.vmCredential,
            diskResp.resourceId
          ),
          jobControl
        )
      }
      _ <- wsmDao.createVm(createVmRequest, auth)
    } yield ()

  override def startAndMonitorRuntime(runtime: Runtime, azureCloudContext: AzureCloudContext)(implicit
    ev: Ask[F, AppContext]
  ): F[Unit] = for {
    ctx <- ev.ask
    monoOpt <- azureVmServiceInterp.startAzureVm(InstanceName(runtime.runtimeName.asString), azureCloudContext)

    _ <- monoOpt match {
      case None =>
        F.raiseError(
          AzureRuntimeStartingError(
            runtime.id,
            s"Starting runtime ${runtime.id} request to Azure failed.",
            ctx.traceId
          )
        )
      case Some(mono) =>
        val task = for {
          _ <- F.blocking(mono.block(Duration.ofMinutes(5)))
          isJupyterUp = jupyterDAO.isProxyAvailable(runtime.cloudContext, runtime.runtimeName)
          _ <- streamUntilDoneOrTimeout(
            isJupyterUp,
            config.createVmPollConfig.maxAttempts,
            config.createVmPollConfig.interval,
            s"Jupyter was not running within ${config.createVmPollConfig.maxAttempts} attempts with ${config.createVmPollConfig.interval} delay"
          )
          _ <- clusterQuery.updateClusterStatus(runtime.id, RuntimeStatus.Running, ctx.now).transaction
          _ <- logger.info(ctx.loggingCtx)("runtime is ready")
        } yield ()
        asyncTasks.offer(
          Task(
            ctx.traceId,
            task,
            Some(e =>
              handleAzureRuntimeStartError(
                AzureRuntimeStartingError(
                  runtime.id,
                  s"Starting runtime ${runtime.projectNameString} failed. Cause: ${e.getMessage}",
                  ctx.traceId
                ),
                ctx.now
              )
            ),
            ctx.now,
            "startRuntime"
          )
        )
    }
  } yield ()

  override def stopAndMonitorRuntime(runtime: Runtime, azureCloudContext: AzureCloudContext)(implicit
    ev: Ask[F, AppContext]
  ): F[Unit] = for {
    ctx <- ev.ask
    monoOpt <- azureVmServiceInterp.stopAzureVm(InstanceName(runtime.runtimeName.asString), azureCloudContext)
    _ <- monoOpt match {
      case None =>
        F.raiseError(
          AzureRuntimeStoppingError(
            runtime.id,
            s"Stopping runtime ${runtime.id} request to Azure failed.",
            ctx.traceId
          )
        )
      case Some(mono) =>
        val task = for {
          _ <- F.blocking(mono.block(Duration.ofMinutes(5)))
          _ <- clusterQuery.updateClusterStatus(runtime.id, RuntimeStatus.Stopped, ctx.now).transaction
          _ <- logger.info(ctx.loggingCtx)("runtime is stopped")
          _ <- welderDao
            .flushCache(runtime.cloudContext, runtime.runtimeName)
            .handleErrorWith(e =>
              logger.error(ctx.loggingCtx, e)(
                s"Failed to flush welder cache for ${runtime.projectNameString}"
              )
            )
            .whenA(runtime.welderEnabled)
        } yield ()
        asyncTasks.offer(
          Task(
            ctx.traceId,
            task,
            Some(e =>
              handleAzureRuntimeStopError(
                AzureRuntimeStoppingError(
                  runtime.id,
                  s"stopping runtime ${runtime.projectNameString} failed. Cause: ${e.getMessage}",
                  ctx.traceId
                ),
                ctx.now
              )
            ),
            ctx.now,
            "startRuntime"
          )
        )
    }
  } yield ()

  private def createStorageContainer(params: CreateAzureRuntimeParams, auth: Authorization)(implicit
    ev: Ask[F, AppContext]
  ): F[(ContainerName, WsmControlledResourceId)] = {
    val stagingContainerName = ContainerName(s"ls-${params.runtime.runtimeName.asString}")
    val storageContainerCommonFields = getCommonFields(
      ControlledResourceName(s"c-${stagingContainerName.value}"),
      "leonardo staging bucket",
      params.runtime.auditInfo.creator,
      None
    )
    for {
      ctx <- ev.ask[AppContext]
      resp <- wsmDao.createStorageContainer(
        CreateStorageContainerRequest(
          params.workspaceId,
          storageContainerCommonFields,
          StorageContainerRequest(stagingContainerName)
        ),
        auth
      )
      _ <- controlledResourceQuery
        .save(params.runtime.id, resp.resourceId, WsmResourceType.AzureStorageContainer)
        .transaction
      _ <- clusterQuery
        .updateStagingBucket(
          params.runtime.id,
          Some(StagingBucket.Azure(stagingContainerName)),
          ctx.now
        )
        .transaction
    } yield (stagingContainerName, resp.resourceId)
  }

  private def createDisk(params: CreateAzureRuntimeParams, leoAuth: Authorization)(implicit
    ev: Ask[F, AppContext]
  ): F[CreateDiskResponse] =
    params.useExistingDisk match {

      // if using existing disk, check conditions and update tables
      case true =>
        for {
          ctx <- ev.ask
          diskOpt <- persistentDiskQuery.getById(params.runtimeConfig.persistentDiskId).transaction
          disk <- F.fromOption(
            diskOpt,
            new RuntimeException(s"Disk id:${params.runtimeConfig.persistentDiskId} not found")
          )
          resourceId <- F.fromOption(
            disk.wsmResourceId,
            AzureRuntimeCreationError(
              params.runtime.id,
              params.workspaceId,
              s"No associated resourceId found for Disk id:${params.runtimeConfig.persistentDiskId.value}",
              params.useExistingDisk
            )
          )
          diskResourceOpt <- controlledResourceQuery
            .getWsmRecordFromResourceId(resourceId, WsmResourceType.AzureDisk)
            .transaction
          wsmDisk <- F.fromOption(
            diskResourceOpt,
            AzureRuntimeCreationError(
              params.runtime.id,
              params.workspaceId,
              s"WSMResource:${resourceId} not found for disk id:${params.runtimeConfig.persistentDiskId.value}",
              params.useExistingDisk
            )
          )
          // set runtime to new runtimeId for disk
          _ <- controlledResourceQuery
            .updateRuntime(wsmDisk.resourceId, WsmResourceType.AzureDisk, params.runtime.id)
            .transaction
          diskResp = CreateDiskResponse(wsmDisk.resourceId)
        } yield diskResp

      // if not using existing disk, send a create disk request
      case false =>
        for {
          ctx <- ev.ask
          diskOpt <- persistentDiskQuery.getById(params.runtimeConfig.persistentDiskId).transaction
          disk <- F.fromOption(
            diskOpt,
            AzureRuntimeCreationError(params.runtime.id,
                                      params.workspaceId,
                                      s"Disk ${params.runtimeConfig.persistentDiskId.value} not found",
                                      params.useExistingDisk
            )
          )
          common = getCommonFields(ControlledResourceName(disk.name.value),
                                   config.runtimeDefaults.diskControlledResourceDesc,
                                   params.runtime.auditInfo.creator,
                                   None
          )
          request: CreateDiskRequest = CreateDiskRequest(
            params.workspaceId,
            common,
            CreateDiskRequestData(
              // TODO: AzureDiskName should go away once DiskName is no longer coupled to google2 disk service
              AzureDiskName(disk.name.value),
              disk.size,
              params.runtimeConfig.region
            )
          )
          diskResp <- wsmDao.createDisk(request, leoAuth)
          _ <- controlledResourceQuery
            .save(params.runtime.id, diskResp.resourceId, WsmResourceType.AzureDisk)
            .transaction
          _ <- persistentDiskQuery.updateStatus(disk.id, DiskStatus.Ready, ctx.now).transaction
          _ <- persistentDiskQuery.updateWSMResourceId(disk.id, diskResp.resourceId, ctx.now).transaction
        } yield diskResp
    }

  private def getCommonFields(name: ControlledResourceName,
                              resourceDesc: String,
                              userEmail: WorkbenchEmail,
                              resourceId: Option[WsmControlledResourceId]
  ) =
    ControlledResourceCommonFields(
      name,
      ControlledResourceDescription(resourceDesc),
      CloningInstructions.Nothing,
      AccessScope.PrivateAccess,
      ManagedBy.Application,
      Some(
        PrivateResourceUser(
          userEmail,
          ControlledResourceIamRole.Writer
        )
      ),
      resourceId
    )

  private def monitorCreateRuntime(params: PollRuntimeParams)(implicit ev: Ask[F, AppContext]): F[Unit] = {
    implicit val wsmCreateVmDoneCheckable: DoneCheckable[GetCreateVmJobResult] = (v: GetCreateVmJobResult) =>
      v.jobReport.status.equals(WsmJobStatus.Succeeded) || v.jobReport.status == WsmJobStatus.Failed
    for {
      ctx <- ev.ask

      auth <- samDAO.getLeoAuthToken
      getWsmJobResult = wsmDao.getCreateVmJobResult(GetJobResultRequest(params.workspaceId, params.jobId), auth)

      cloudContext = params.runtime.cloudContext match {
        case _: CloudContext.Gcp =>
          throw PubsubHandleMessageError.ClusterError(params.runtime.id,
                                                      ctx.traceId,
                                                      "Azure runtime should not have GCP cloud context"
          )
        case x: CloudContext.Azure => x
      }

      isJupyterUp = jupyterDAO.isProxyAvailable(cloudContext, params.runtime.runtimeName)
      isWelderUp = welderDao.isProxyAvailable(cloudContext, params.runtime.runtimeName)

      taskToRun = for {
        _ <- F.sleep(
          config.createVmPollConfig.initialDelay
        ) // it takes a while to create Azure VM. Hence sleep sometime before we start polling WSM
        // first poll the WSM createVm job for completion
        resp <- streamFUntilDone(
          getWsmJobResult,
          config.createVmPollConfig.maxAttempts,
          config.createVmPollConfig.interval
        ).compile.lastOrError
        _ <- resp.jobReport.status match {
          case WsmJobStatus.Failed =>
            F.raiseError[Unit](
              AzureRuntimeCreationError(
                params.runtime.id,
                params.workspaceId,
                s"Wsm createVm job failed due to ${resp.errorReport.map(_.message).getOrElse("unknown")}",
                params.useExistingDisk
              )
            )
          case WsmJobStatus.Running =>
            F.raiseError[Unit](
              AzureRuntimeCreationError(
                params.runtime.id,
                params.workspaceId,
                s"Wsm createVm job was not completed within ${config.createVmPollConfig.maxAttempts} attempts with ${config.createVmPollConfig.interval} delay",
                params.useExistingDisk
              )
            )
          case WsmJobStatus.Succeeded =>
            val hostIp = s"${params.relayNamespace.value}.servicebus.windows.net"

            for {
              _ <- clusterQuery.updateClusterHostIp(params.runtime.id, Some(IP(hostIp)), ctx.now).transaction
              // then poll the azure VM for Running status, retrieving the final azure representation
              _ <- streamUntilDoneOrTimeout(
                isJupyterUp,
                config.createVmPollConfig.maxAttempts,
                config.createVmPollConfig.interval,
                s"Jupyter was not running within ${config.createVmPollConfig.maxAttempts} attempts with ${config.createVmPollConfig.interval} delay"
              )
              _ <- streamUntilDoneOrTimeout(
                isWelderUp,
                config.createVmPollConfig.maxAttempts,
                config.createVmPollConfig.interval,
                s"Welder was not running within ${config.createVmPollConfig.maxAttempts} attempts with ${config.createVmPollConfig.interval} delay"
              )
              _ <- clusterQuery.setToRunning(params.runtime.id, IP(hostIp), ctx.now).transaction
              _ <- logger.info(ctx.loggingCtx)("runtime is ready")
            } yield ()
        }
      } yield ()

      _ <- asyncTasks.offer(
        Task(
          ctx.traceId,
          taskToRun,
          Some(e =>
            handleAzureRuntimeCreationError(
              AzureRuntimeCreationError(params.runtime.id, params.workspaceId, e.getMessage, params.useExistingDisk),
              ctx.now
            )
          ),
          ctx.now,
          "createAzureRuntime"
        )
      )
    } yield ()
  }

  override def deleteAndPollRuntime(msg: DeleteAzureRuntimeMessage)(implicit ev: Ask[F, AppContext]): F[Unit] = {
    implicit val azureRuntimeDeletingDoneCheckable: DoneCheckable[Option[VirtualMachine]] =
      (v: Option[VirtualMachine]) => v.isEmpty
    for {
      ctx <- ev.ask

      runtimeOpt <- dbRef.inTransaction(clusterQuery.getClusterById(msg.runtimeId))
      runtime <- F.fromOption(runtimeOpt, PubsubHandleMessageError.ClusterNotFound(msg.runtimeId, msg))
      auth <- samDAO.getLeoAuthToken

      deleteJobId = WsmJobId(s"delete-vm-${ctx.traceId.asString.take(10)}")
      _ <- msg.wsmResourceId.fold(
        logger
          .info(ctx.loggingCtx)(
            s"No VM wsmResourceId found for delete azure runtime msg $msg. No-op for wsmDao.deleteVm."
          )
      ) { wsmResourceId =>
        wsmDao
          .deleteVm(
            DeleteWsmResourceRequest(
              msg.workspaceId,
              wsmResourceId,
              DeleteControlledAzureResourceRequest(
                WsmJobControl(deleteJobId)
              )
            ),
            auth
          )
          .void
          .adaptError(e =>
            AzureRuntimeDeletionError(
              runtime.id,
              msg.workspaceId,
              s"${ctx.traceId.asString} | WSM call to delete runtime failed due to ${e.getMessage}. Please retry delete again"
            )
          )
      }

      stagingBucketResourceOpt <- controlledResourceQuery
        .getWsmRecordForRuntime(runtime.id, WsmResourceType.AzureStorageContainer)
        .transaction
      _ <- stagingBucketResourceOpt.fold(
        logger
          .info(ctx.loggingCtx)(
            s"No Storage Container wsmResourceId found for delete azure runtime msg $msg. No-op for wsmDao.deleteStorageContainer."
          )
      ) { stagingBucketResourceId =>
        wsmDao
          .deleteStorageContainer(
            DeleteWsmResourceRequest(
              msg.workspaceId,
              stagingBucketResourceId.resourceId,
              DeleteControlledAzureResourceRequest(
                WsmJobControl(WsmJobId(s"del-staging-${ctx.traceId.asString.take(10)}"))
              )
            ),
            auth
          )
          .void
      }

      // Delete hybrid connection for this VM
      leoAuth <- samDAO.getLeoAuthToken
      runtimeConfig <- RuntimeConfigQueries.getRuntimeConfig(runtime.runtimeConfigId).transaction
      azureRuntimeConfig <- runtimeConfig match {
        case x: RuntimeConfig.AzureConfig => F.pure(x)
        case _ =>
          F.raiseError(
            ClusterError(msg.runtimeId, ctx.traceId, s"Runtime should have Azure config, but it doesn't")
          )
      }
      relayNamespaceOpt <- wsmDao.getRelayNamespace(msg.workspaceId, azureRuntimeConfig.region, leoAuth)
      cloudContext <- runtime.cloudContext match {
        case _: CloudContext.Gcp =>
          F.raiseError[AzureCloudContext](
            PubsubHandleMessageError.ClusterError(msg.runtimeId,
                                                  ctx.traceId,
                                                  "Azure runtime should not have GCP cloud context"
            )
          )
        case x: CloudContext.Azure => F.pure(x.value)
      }
      _ <- relayNamespaceOpt.traverse(ns =>
        azureRelay.deleteRelayHybridConnection(
          ns,
          RelayHybridConnectionName(runtime.runtimeName.asString),
          cloudContext
        )
      )

      getDeleteJobResultOpt = wsmDao.getDeleteVmJobResult(
        GetJobResultRequest(msg.workspaceId, deleteJobId),
        auth
      )

      taskToRun = for {
        // We need to wait until WSM deletion job to be done because if the VM still exists, we won't be able to delete disk, and networks
        respOpt <- streamFUntilDone(
          getDeleteJobResultOpt,
          config.deleteVmPollConfig.maxAttempts,
          config.deleteVmPollConfig.interval
        ).compile.lastOrError

        deleteDiskAction = msg.diskIdToDelete.traverse { _ =>
          for {
            diskResourceOpt <- controlledResourceQuery
              .getWsmRecordForRuntime(runtime.id, WsmResourceType.AzureDisk)
              .transaction
            _ <- logger
              .info(ctx.loggingCtx)(
                s"No disk resource found for delete azure runtime msg $msg. No-op for wsmDao.deleteDisk."
              )
              .whenA(diskResourceOpt.isEmpty)
            _ <- diskResourceOpt.traverse { disk =>
              wsmDao.deleteDisk(
                DeleteWsmResourceRequest(
                  msg.workspaceId,
                  disk.resourceId,
                  DeleteControlledAzureResourceRequest(
                    WsmJobControl(WsmJobId(s"delete-disk-${ctx.traceId.asString.take(10)}"))
                  )
                ),
                auth
              )
            }.void

            _ <- msg.diskIdToDelete.traverse(diskId =>
              for {
                _ <- dbRef.inTransaction(persistentDiskQuery.updateStatus(diskId, DiskStatus.Deleted, ctx.now))
                _ <- logger.info(ctx.loggingCtx)(s"runtime disk ${diskId} is deleted successfully")
              } yield ()
            )
          } yield ()
        }.void

        _ <- respOpt match {
          case Some(resp) =>
            resp.jobReport.status match {
              case WsmJobStatus.Succeeded =>
                for {
                  _ <- dbRef.inTransaction(clusterQuery.updateClusterStatus(runtime.id, RuntimeStatus.Deleted, ctx.now))
                  _ <- logger.info(ctx.loggingCtx)(s"runtime ${msg.runtimeId} is deleted successfully")
                  _ <- deleteDiskAction
                } yield ()
              case WsmJobStatus.Failed =>
                F.raiseError[Unit](
                  AzureRuntimeDeletionError(
                    msg.runtimeId,
                    msg.workspaceId,
                    s"WSM delete VM job failed due to ${resp.errorReport.map(_.message).getOrElse("unknown")}"
                  )
                )
              case WsmJobStatus.Running =>
                F.raiseError[Unit](
                  AzureRuntimeDeletionError(
                    msg.runtimeId,
                    msg.workspaceId,
                    s"WSM delete VM job was not completed within ${config.deleteVmPollConfig.maxAttempts} attempts with ${config.deleteVmPollConfig.interval} delay"
                  )
                )
            }
          case None => deleteDiskAction
        }
      } yield ()

      _ <- asyncTasks.offer(
        Task(
          ctx.traceId,
          taskToRun,
          Some { e =>
            handleAzureRuntimeDeletionError(
              AzureRuntimeDeletionError(msg.runtimeId,
                                        msg.workspaceId,
                                        s"Fail to delete runtime due to ${e.getMessage}"
              )
            )
          },
          ctx.now,
          "deleteAzureRuntime"
        )
      )
    } yield ()
  }

  def handleAzureRuntimeDeletionError(e: AzureRuntimeDeletionError)(implicit
    ev: Ask[F, AppContext]
  ): F[Unit] = for {
    ctx <- ev.ask
    _ <- logger.error(ctx.loggingCtx, e)(s"Failed to delete Azure VM ${e.runtimeId}")
    _ <- clusterErrorQuery
      .save(e.runtimeId, RuntimeError(e.errorMsg.take(1024), None, ctx.now, traceId = Some(ctx.traceId)))
      .transaction
    _ <- clusterQuery.updateClusterStatus(e.runtimeId, RuntimeStatus.Error, ctx.now).transaction
  } yield ()

  def handleAzureRuntimeStartError(e: AzureRuntimeStartingError, now: Instant)(implicit
    ev: Ask[F, AppContext]
  ): F[Unit] =
    for {
      ctx <- ev.ask
      _ <- logger.error(ctx.loggingCtx)(s"Failed to start Azure VM ${e.runtimeId}")
      _ <- clusterErrorQuery
        .save(e.runtimeId, RuntimeError(e.errorMsg.take(1024), None, now))
        .transaction
    } yield ()

  def handleAzureRuntimeStopError(e: AzureRuntimeStoppingError, now: Instant)(implicit
    ev: Ask[F, AppContext]
  ): F[Unit] =
    for {
      ctx <- ev.ask
      _ <- logger.error(ctx.loggingCtx)(s"Failed to stop Azure VM ${e.runtimeId}")
      _ <- clusterErrorQuery
        .save(e.runtimeId, RuntimeError(e.errorMsg.take(1024), None, now))
        .transaction
    } yield ()

  def handleAzureRuntimeCreationError(e: AzureRuntimeCreationError, now: Instant)(implicit
    ev: Ask[F, AppContext]
  ): F[Unit] =
    for {
      ctx <- ev.ask
      _ <- logger.error(ctx.loggingCtx, e)(s"Failed to create Azure VM ${e.runtimeId}")
      _ <- clusterErrorQuery
        .save(e.runtimeId, RuntimeError(e.errorMsg.take(1024), None, now))
        .transaction
      _ <- clusterQuery.updateClusterStatus(e.runtimeId, RuntimeStatus.Error, now).transaction

      auth <- samDAO.getLeoAuthToken

      // only delete disk on error if not using existing disk
      _ <- e.useExistingDisk match {
        case false =>
          for {
            diskResourceOpt <- controlledResourceQuery
              .getWsmRecordForRuntime(e.runtimeId, WsmResourceType.AzureDisk)
              .transaction
            _ <- diskResourceOpt.traverse { disk =>
              wsmDao.deleteDisk(
                DeleteWsmResourceRequest(
                  e.workspaceId,
                  disk.resourceId,
                  DeleteControlledAzureResourceRequest(
                    WsmJobControl(WsmJobId(s"delete-disk-${ctx.traceId.asString.take(10)}"))
                  )
                ),
                auth
              )
            }.void
            _ <- clusterQuery.updateDiskStatus(e.runtimeId, now).transaction
          } yield ()

        case true => F.unit
      }

      networkResourceOpt <- controlledResourceQuery
        .getWsmRecordForRuntime(e.runtimeId, WsmResourceType.AzureNetwork)
        .transaction
      _ <- networkResourceOpt.traverse { network =>
        wsmDao.deleteNetworks(
          DeleteWsmResourceRequest(
            e.workspaceId,
            network.resourceId,
            DeleteControlledAzureResourceRequest(
              WsmJobControl(WsmJobId(s"delete-networks-${ctx.traceId.asString.take(10)}"))
            )
          ),
          auth
        )
      }.void
    } yield ()

  override def createAndPollApp(appId: AppId,
                                appName: AppName,
                                workspaceId: WorkspaceId,
                                cloudContext: AzureCloudContext,
                                landingZoneResources: LandingZoneResources,
                                storageContainer: Option[StorageContainerResponse]
  )(implicit
    ev: Ask[F, AppContext]
  ): F[Unit] =
    for {
      ctx <- ev.ask
      params = CreateAKSAppParams(appId, appName, workspaceId, cloudContext, landingZoneResources, storageContainer)
      _ <- aksAlgebra.createAndPollApp(params).adaptError { case e =>
        PubsubKubernetesError(
          AppError(
            s"Error creating Azure app with id ${appId.id} and cloudContext ${cloudContext.asString}: ${e.getMessage}",
            ctx.now,
            ErrorAction.CreateApp,
            ErrorSource.App,
            None,
            Some(ctx.traceId)
          ),
          Some(appId),
          false,
          None,
          None,
          None
        )
      }
    } yield ()

  override def deleteApp(
    appId: AppId,
    appName: AppName,
    workspaceId: WorkspaceId,
    landingZoneResources: LandingZoneResources,
    cloudContext: AzureCloudContext
  )(implicit
    ev: Ask[F, AppContext]
  ): F[Unit] =
    for {
      ctx <- ev.ask
      params = DeleteAKSAppParams(appName, workspaceId, landingZoneResources, cloudContext)
      _ <- aksAlgebra.deleteApp(params).adaptError { case e =>
        PubsubKubernetesError(
          AppError(
            s"Error deleting Azure app with id ${appId.id} and cloudContext ${cloudContext.asString}: ${e.getMessage}",
            ctx.now,
            ErrorAction.DeleteApp,
            ErrorSource.App,
            None,
            Some(ctx.traceId)
          ),
          Some(appId),
          false,
          None,
          None,
          None
        )
      }
    } yield ()
}
