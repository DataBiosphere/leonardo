package org.broadinstitute.dsde.workbench
package leonardo
package util

import cats.Parallel
import cats.effect.Async
import cats.effect.std.Queue
import cats.mtl.Ask
import cats.syntax.all._
import com.azure.resourcemanager.compute.models.VirtualMachineSizeTypes
import org.broadinstitute.dsde.workbench.azure._
import org.broadinstitute.dsde.workbench.google2.{streamFUntilDone, streamUntilDoneOrTimeout}
import org.broadinstitute.dsde.workbench.leonardo.AsyncTaskProcessor.Task
import org.broadinstitute.dsde.workbench.leonardo.config.{ApplicationConfig, ContentSecurityPolicyConfig, RefererConfig}
import org.broadinstitute.dsde.workbench.leonardo.dao._
import org.broadinstitute.dsde.workbench.leonardo.db._
import org.broadinstitute.dsde.workbench.leonardo.http.{ctxConversion, dbioToIO}
import org.broadinstitute.dsde.workbench.leonardo.monitor.LeoPubsubMessage.{
  CreateAzureRuntimeMessage,
  DeleteAzureRuntimeMessage,
  DeleteDiskV2Message
}
import org.broadinstitute.dsde.workbench.leonardo.monitor.PubsubHandleMessageError
import org.broadinstitute.dsde.workbench.leonardo.monitor.PubsubHandleMessageError._
import org.broadinstitute.dsde.workbench.model.{IP, WorkbenchEmail}
import org.broadinstitute.dsde.workbench.util2.InstanceName
import org.broadinstitute.dsp.ChartVersion
import org.http4s.headers.Authorization
import org.typelevel.log4cats.StructuredLogger

import java.time.{Duration, Instant}
import java.util.UUID
import scala.concurrent.ExecutionContext

class AzurePubsubHandlerInterp[F[_]: Parallel](
  config: AzurePubsubHandlerConfig,
  applicationConfig: ApplicationConfig,
  contentSecurityPolicyConfig: ContentSecurityPolicyConfig,
  asyncTasks: Queue[F, Task[F]],
  wsmDao: WsmDao[F],
  samDAO: SamDAO[F],
  welderDao: WelderDAO[F],
  jupyterDAO: JupyterDAO[F],
  azureRelay: AzureRelayService[F],
  azureVmServiceInterp: AzureVmService[F],
  aksAlgebra: AKSAlgebra[F],
  refererConfig: RefererConfig
)(implicit val executionContext: ExecutionContext, dbRef: DbReference[F], logger: StructuredLogger[F], F: Async[F])
    extends AzurePubsubHandlerAlgebra[F] {

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
      createVmJobId = WsmJobId(s"create-vm-${runtime.id.toString.take(10)}")
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
      // Construct the workspace storage container URL which will be passed to the JupyterLab environment variables.
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
          wsStorageContainerUrl,
          applicationConfig.leoUrlBase,
          params.runtime.runtimeName.asString,
          s"'${refererConfig.validHosts.mkString("','")}'"
        )

        val cmdToExecute =
          s"touch /var/log/azure_vm_init_script.log && chmod 400 /var/log/azure_vm_init_script.log &&" +
            s"echo \"${contentSecurityPolicyConfig.asString}\" > csp.txt && bash azure_vm_init_script.sh ${arguments
                .map(s => s"'$s'")
                .mkString(" ")} > /var/log/azure_vm_init_script.log"

        CreateVmRequest(
          params.workspaceId,
          vmCommon,
          CreateVmRequestData(
            params.runtime.runtimeName,
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
  ): F[CreateDiskResponse] = for {

    diskId <- F.fromOption(
      params.runtimeConfig.persistentDiskId,
      AzureRuntimeCreationError(
        params.runtime.id,
        params.workspaceId,
        s"No associated diskId found for runtime:${params.runtime.id}",
        params.useExistingDisk
      )
    )

    resp <- params.useExistingDisk match {
      // if using existing disk, check conditions and update tables
      case true =>
        for {
          ctx <- ev.ask
          diskOpt <- persistentDiskQuery.getById(diskId).transaction
          disk <- F.fromOption(
            diskOpt,
            new RuntimeException(s"Disk id:${diskId.value} not found for runtime:${params.runtime.id}")
          )
          resourceId <- F.fromOption(
            disk.wsmResourceId,
            AzureRuntimeCreationError(
              params.runtime.id,
              params.workspaceId,
              s"No associated resourceId found for Disk id:${diskId.value}",
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
              s"WSMResource:${resourceId.value} not found for disk id:${diskId.value}",
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
          diskOpt <- persistentDiskQuery.getById(diskId).transaction
          disk <- F.fromOption(
            diskOpt,
            AzureRuntimeCreationError(params.runtime.id,
                                      params.workspaceId,
                                      s"Disk ${diskId} not found",
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
              disk.size
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
  } yield resp

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
              now <- nowInstant
              _ <- clusterQuery.updateClusterHostIp(params.runtime.id, Some(IP(hostIp)), now).transaction
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
              _ <- clusterQuery.setToRunning(params.runtime.id, IP(hostIp), now).transaction
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
    implicit val wsmDeleteVmDoneCheckable: DoneCheckable[GetDeleteJobResult] = (v: GetDeleteJobResult) =>
      v.jobReport.status.equals(WsmJobStatus.Succeeded) || v.jobReport.status == WsmJobStatus.Failed
    for {
      ctx <- ev.ask

      runtimeOpt <- dbRef.inTransaction(clusterQuery.getClusterById(msg.runtimeId))
      runtime <- F.fromOption(runtimeOpt, PubsubHandleMessageError.ClusterNotFound(msg.runtimeId, msg))
      auth <- samDAO.getLeoAuthToken

      // Delete the staging storage container in WSM
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
                WsmJobControl(getWsmJobId("del-staging", stagingBucketResourceId.resourceId))
              )
            ),
            auth
          )
          .void
      }

      // Grab the cloud context for the runtime
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

      // Delete hybrid connection for this VM
      _ <- azureRelay.deleteRelayHybridConnection(
        msg.landingZoneResources.relayNamespace,
        RelayHybridConnectionName(runtime.runtimeName.asString),
        cloudContext
      )

      deleteDiskAction = msg.diskIdToDelete.traverse { diskId =>
        for {
          _ <- deleteDiskResource(Left(runtime.id), msg.workspaceId, auth)
          _ <- dbRef.inTransaction(persistentDiskQuery.delete(diskId, ctx.now))
          _ <- logger.info(ctx.loggingCtx)(s"runtime disk ${diskId} is deleted successfully")
        } yield ()
      }.void

      // Delete the VM in WSM
      _ <- msg.wsmResourceId.fold(
        for {
          // Error'd runtimes might not have a WSM resourceId and therefore no WsmJobStatus.
          // We still want deletion to succeed in this case.
          _ <- logger
            .info(ctx.loggingCtx)(
              s"No VM wsmResourceId found for delete azure runtime msg $msg. No-op for wsmDao.deleteVm."
            )

          // if no wsmResource to delete, delete the disk and set runtime to deleted
          _ <- deleteDiskAction
          _ <- dbRef.inTransaction(clusterQuery.updateClusterStatus(runtime.id, RuntimeStatus.Deleted, ctx.now))
          _ <- logger.info(ctx.loggingCtx)(
            s"runtime ${msg.runtimeId} with name ${runtime.runtimeName.asString} is deleted successfully"
          )
        } yield ()
      ) { wsmResourceId =>
        for {
          _ <- wsmDao
            .deleteVm(
              DeleteWsmResourceRequest(
                msg.workspaceId,
                wsmResourceId,
                DeleteControlledAzureResourceRequest(
                  WsmJobControl(getWsmJobId("delete-vm", wsmResourceId))
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

          getDeleteJobResult = wsmDao.getDeleteVmJobResult(
            GetJobResultRequest(msg.workspaceId, getWsmJobId("delete-vm", wsmResourceId)),
            auth
          )

          // Poll for VM deletion
          taskToRun = for {
            // We need to wait until WSM deletion job to be done because if the VM still exists, we won't be able to delete disk
            resp <- streamFUntilDone(
              getDeleteJobResult,
              config.deleteVmPollConfig.maxAttempts,
              config.deleteVmPollConfig.interval
            ).compile.lastOrError

            _ <- resp.jobReport.status match {
              case WsmJobStatus.Succeeded =>
                for {
                  _ <- deleteDiskAction
                  _ <- dbRef.inTransaction(clusterQuery.completeDeletion(runtime.id, ctx.now))
                  _ <- logger.info(ctx.loggingCtx)(s"runtime ${msg.runtimeId} is deleted successfully")
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

  override def updateAndPollApp(appId: AppId,
                                appName: AppName,
                                appChartVersion: ChartVersion,
                                workspaceId: Option[WorkspaceId],
                                cloudContext: AzureCloudContext
  )(implicit
    ev: Ask[F, AppContext]
  ): F[Unit] =
    for {
      ctx <- ev.ask
      params = UpdateAKSAppParams(appId, appName, appChartVersion, workspaceId, cloudContext)
      _ <- aksAlgebra.updateAndPollApp(params).adaptError { case e =>
        PubsubKubernetesError(
          AppError(
            s"Error updating Azure app with id ${appId.id} and cloudContext ${cloudContext.asString}: ${e.getMessage}",
            ctx.now,
            ErrorAction.UpdateApp,
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

  private def deleteDiskResource(id: Either[Long,
                                            WsmControlledResourceId
                                 ], // can be either runtimeId or diskResourceId in order to get WSMcontrolled resource
                                 workspaceId: WorkspaceId,
                                 auth: Authorization
  )(implicit
    ev: Ask[F, AppContext]
  ): F[Unit] =
    for {
      ctx <- ev.ask
      diskResourceOpt <- id match {
        case Left(runtimeId) =>
          controlledResourceQuery
            .getWsmRecordForRuntime(runtimeId, WsmResourceType.AzureDisk)
            .transaction
        case Right(diskResourceId) =>
          controlledResourceQuery
            .getWsmRecordFromResourceId(diskResourceId, WsmResourceType.AzureDisk)
            .transaction
      }
      diskResource <- F.fromOption(
        diskResourceOpt,
        AzureDiskResourceDeletionError(id,
                                       workspaceId,
                                       "No disk resource found for delete azure disk. No-op for wsmDao.deleteDisk."
        )
      )
      jobId = getWsmJobId("delete-disk", diskResource.resourceId)

      _ <- diskResourceOpt.traverse { disk =>
        for {
          _ <- logger.info(ctx.loggingCtx)(s"Sending WSM delete message for disk resource ${disk.resourceId.value}")
          _ <- wsmDao
            .deleteDisk(
              DeleteWsmResourceRequest(
                workspaceId,
                disk.resourceId,
                DeleteControlledAzureResourceRequest(
                  WsmJobControl(jobId)
                )
              ),
              auth
            )
            .void
            .adaptError(e =>
              AzureDiskDeletionError(
                disk.resourceId,
                workspaceId,
                s"${ctx.traceId.asString} | WSM call to delete disk failed due to ${e.getMessage}. Please retry delete again"
              )
            )
        } yield ()
      }.void
    } yield ()

  override def deleteDisk(msg: DeleteDiskV2Message)(implicit ev: Ask[F, AppContext]): F[Unit] = {
    implicit val wsmDeleteDiskDoneCheckable: DoneCheckable[GetDeleteJobResult] = (v: GetDeleteJobResult) =>
      v.jobReport.status.equals(WsmJobStatus.Succeeded) || v.jobReport.status == WsmJobStatus.Failed
    for {
      ctx <- ev.ask
      auth <- samDAO.getLeoAuthToken

      wsmResourceId <- F.fromOption(
        msg.wsmResourceId,
        DiskDeletionError(
          msg.diskId,
          msg.workspaceId,
          s"No associated WsmResourceId found for Azure disk"
        )
      )

      _ <- deleteDiskResource(Right(wsmResourceId), msg.workspaceId, auth)

      deleteJobId = getWsmJobId("delete-disk", wsmResourceId)
      getDeleteJobResult = wsmDao.getDeleteDiskJobResult(
        GetJobResultRequest(msg.workspaceId, deleteJobId),
        auth
      )

      taskToRun = for {
        // We need to wait until WSM deletion job to be done to update the database
        resp <- streamFUntilDone(
          getDeleteJobResult,
          config.deleteDiskPollConfig.maxAttempts,
          config.deleteDiskPollConfig.interval
        ).compile.lastOrError

        _ <- resp.jobReport.status match {
          case WsmJobStatus.Succeeded =>
            for {
              _ <- logger.info(ctx.loggingCtx)(s"disk ${msg.diskId.value} is deleted successfully")
              _ <- dbRef.inTransaction(persistentDiskQuery.delete(msg.diskId, ctx.now))
            } yield ()
          case WsmJobStatus.Failed =>
            F.raiseError[Unit](
              DiskDeletionError(
                msg.diskId,
                msg.workspaceId,
                s"WSM delete disk job failed due to ${resp.errorReport.map(_.message).getOrElse("unknown")}"
              )
            )
          case WsmJobStatus.Running =>
            F.raiseError[Unit](
              DiskDeletionError(
                msg.diskId,
                msg.workspaceId,
                s"WSM delete disk job failed due to ${resp.errorReport.map(_.message).getOrElse("unknown")}"
              )
            )
        }
      } yield ()

      _ <- asyncTasks.offer(
        Task(
          ctx.traceId,
          taskToRun,
          Some { e =>
            handleAzureDiskDeletionError(
              DiskDeletionError(msg.diskId, msg.workspaceId, s"Fail to delete disk due to ${e.getMessage}")
            )
          },
          ctx.now,
          "deleteDiskV2"
        )
      )
    } yield ()
  }

  def handleAzureDiskDeletionError(e: DiskDeletionError)(implicit
    ev: Ask[F, AppContext]
  ): F[Unit] = for {
    ctx <- ev.ask
    _ <- logger.error(ctx.loggingCtx, e)(s"Failed to delete Azure disk ${e.diskId}")
    _ <- dbRef.inTransaction(persistentDiskQuery.updateStatus(e.diskId, DiskStatus.Error, ctx.now))
  } yield ()

  def getWsmJobId(jobName: String, resourceId: WsmControlledResourceId): WsmJobId = WsmJobId(
    s"${jobName}-${resourceId.value.toString.take(10)}"
  )
}
