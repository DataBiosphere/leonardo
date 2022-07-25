package org.broadinstitute.dsde.workbench
package leonardo
package util

import cats.Parallel
import cats.effect.Async
import cats.effect.std.Queue
import cats.mtl.Ask
import cats.syntax.all._
import com.azure.resourcemanager.compute.models.{VirtualMachine, VirtualMachineSizeTypes}
import org.broadinstitute.dsde.workbench.azure.{AzureCloudContext, AzureRelayService, RelayHybridConnectionName}
import org.broadinstitute.dsde.workbench.google2.{streamFUntilDone, streamUntilDoneOrTimeout}
import org.broadinstitute.dsde.workbench.leonardo.AsyncTaskProcessor.Task
import org.broadinstitute.dsde.workbench.leonardo.SamResourceId.WsmResourceSamResourceId
import org.broadinstitute.dsde.workbench.leonardo.config.ContentSecurityPolicyConfig
import org.broadinstitute.dsde.workbench.leonardo.dao._
import org.broadinstitute.dsde.workbench.leonardo.db._
import org.broadinstitute.dsde.workbench.leonardo.http.dbioToIO
import org.broadinstitute.dsde.workbench.leonardo.monitor.LeoPubsubMessage.{
  CreateAzureRuntimeMessage,
  DeleteAzureRuntimeMessage
}
import org.broadinstitute.dsde.workbench.leonardo.monitor.PubsubHandleMessageError
import org.broadinstitute.dsde.workbench.leonardo.monitor.PubsubHandleMessageError.{
  AzureRuntimeCreationError,
  AzureRuntimeDeletionError,
  ClusterError
}
import org.broadinstitute.dsde.workbench.model.{IP, WorkbenchEmail}
import org.http4s.headers.Authorization
import org.typelevel.log4cats.StructuredLogger

import java.time.Instant
import java.util.UUID
import scala.concurrent.ExecutionContext
import org.broadinstitute.dsde.workbench.leonardo.http.ctxConversion

class AzurePubsubHandlerInterp[F[_]: Parallel](
  config: AzurePubsubHandlerConfig,
  contentSecurityPolicyConfig: ContentSecurityPolicyConfig,
  asyncTasks: Queue[F, Task[F]],
  wsmDao: WsmDao[F],
  samDAO: SamDAO[F],
  jupyterDAO: JupyterDAO[F],
  azureRelay: AzureRelayService[F]
)(implicit val executionContext: ExecutionContext, dbRef: DbReference[F], logger: StructuredLogger[F], F: Async[F])
    extends AzurePubsubHandlerAlgebra[F] {
  implicit val wsmDeleteVmDoneCheckable: DoneCheckable[Option[GetDeleteJobResult]] = (v: Option[GetDeleteJobResult]) =>
    v match {
      case Some(vv) =>
        vv.jobReport.status.equals(WsmJobStatus.Succeeded) || vv.jobReport.status == WsmJobStatus.Failed
      case None =>
        true
    }

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
      _ <- createRuntime(CreateAzureRuntimeParams(msg.workspaceId,
                                                  runtime,
                                                  msg.relayNamespace,
                                                  azureConfig,
                                                  config.runtimeDefaults.image
                         ),
                         WsmJobControl(createVmJobId)
      )
      _ <- monitorCreateRuntime(
        PollRuntimeParams(msg.workspaceId, runtime, createVmJobId, msg.relayNamespace)
      )
    } yield ()

  /** Creates an Azure VM but doesn't wait for its completion.
   * This includes creation of all child Azure resources (disk, network, ip), and assumes these are created synchronously
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
      primaryKey <- azureRelay.createRelayHybridConnection(params.relayeNamespace, hcName, cloudContext)
      createDiskAction = createDisk(params, auth)
      createNetworkAction = createNetwork(params, auth, params.runtime.runtimeName.asString)

      samResourceId <- F.delay(WsmControlledResourceId(UUID.randomUUID()))
      createVmRequest <- (createDiskAction, createNetworkAction).parMapN { (diskResp, networkResp) =>
        val vmCommon = getCommonFields(
          ControlledResourceName(params.runtime.runtimeName.asString),
          config.runtimeDefaults.vmControlledResourceDesc,
          params.runtime.auditInfo.creator,
          Some(samResourceId)
        )
        val arguments = List(
          params.relayeNamespace.value,
          hcName.value,
          "localhost",
          "listener",
          primaryKey.value,
          config.runtimeDefaults.listenerImage,
          config.samUrl.renderString,
          samResourceId.value.toString,
          "csp.txt"
        )
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
            diskResp.resourceId,
            networkResp.resourceId
          ),
          jobControl
        )
      }
      _ <- wsmDao.createVm(createVmRequest, auth)
    } yield ()

  private def createDisk(params: CreateAzureRuntimeParams, leoAuth: Authorization)(implicit
    ev: Ask[F, AppContext]
  ): F[CreateDiskResponse] =
    for {
      ctx <- ev.ask
      diskOpt <- persistentDiskQuery.getById(params.runtimeConfig.persistentDiskId).transaction
      disk <- F.fromOption(diskOpt, new RuntimeException("no disk found"))
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
    } yield diskResp

  private def createNetwork(
    params: CreateAzureRuntimeParams,
    leoAuth: Authorization,
    nameSuffix: String
  )(implicit ev: Ask[F, AppContext]): F[CreateNetworkResponse] = {
    val common = getCommonFields(ControlledResourceName(s"network-${nameSuffix}"),
                                 config.runtimeDefaults.networkControlledResourceDesc,
                                 params.runtime.auditInfo.creator,
                                 None
    )
    val request: CreateNetworkRequest = CreateNetworkRequest(
      params.workspaceId,
      common,
      CreateNetworkRequestData(
        AzureNetworkName(s"vNet-${nameSuffix}"),
        AzureSubnetName(s"subnet-${nameSuffix}"),
        config.runtimeDefaults.addressSpaceCidr,
        config.runtimeDefaults.subnetAddressCidr,
        params.runtimeConfig.region
      )
    )
    for {
      networkResp <- wsmDao.createNetwork(request, leoAuth)
      _ <- controlledResourceQuery
        .save(params.runtime.id, networkResp.resourceId, WsmResourceType.AzureNetwork)
        .transaction
    } yield networkResp
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
          List(ControlledResourceIamRole.Writer)
        )
      ),
      resourceId
    )

  private def monitorCreateRuntime(params: PollRuntimeParams)(implicit ev: Ask[F, AppContext]): F[Unit] = {
    implicit val isJupyterUpDoneCheckable: DoneCheckable[Boolean] = (v: Boolean) => v
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

      // TODO: this probably isn't super necessary...But we should add a check for pinging jupyter once proxy work is done
      isJupyterUp = jupyterDAO.isProxyAvailable(cloudContext, params.runtime.runtimeName)

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
                s"Wsm createVm job failed due due to ${resp.errorReport.map(_.message).getOrElse("unknown")}"
              )
            )
          case WsmJobStatus.Running =>
            F.raiseError[Unit](
              AzureRuntimeCreationError(
                params.runtime.id,
                params.workspaceId,
                s"Wsm createVm job was not completed within ${config.createVmPollConfig.maxAttempts} attempts with ${config.createVmPollConfig.interval} delay"
              )
            )
          case WsmJobStatus.Succeeded =>
            for {
              _ <- resp.vm.traverse { x =>
                dbRef.inTransaction(
                  controlledResourceQuery.save(
                    params.runtime.id,
                    x.metadata.resourceId,
                    WsmResourceType.AzureVm
                  )
                ) >> dbRef.inTransaction(
                  clusterQuery.updateSamResourceId(params.runtime.id, WsmResourceSamResourceId(x.metadata.resourceId))
                )
              }
              hostIp = s"${params.relayNamespace.value}.servicebus.windows.net"
              _ <- clusterQuery.updateClusterHostIp(params.runtime.id, Some(IP(hostIp)), ctx.now).transaction
              // then poll the azure VM for Running status, retrieving the final azure representation
              _ <- streamUntilDoneOrTimeout(
                isJupyterUp,
                config.createVmPollConfig.maxAttempts,
                config.createVmPollConfig.interval,
                s"Jupyter was not running within ${config.createVmPollConfig.maxAttempts} attempts with ${config.createVmPollConfig.interval} delay"
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
              AzureRuntimeCreationError(params.runtime.id, params.workspaceId, e.getMessage),
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
          .info(ctx.loggingCtx)(s"No wsmResourceId found for delete azure runtime msg $msg. No-op for wsmDao.deleteVm.")
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

        continue = for {
          diskResourceOpt <- controlledResourceQuery
            .getWsmRecordForRuntime(runtime.id, WsmResourceType.AzureDisk)
            .transaction
          _ <- logger
            .info(ctx.loggingCtx)(
              s"No disk resource found for delete azure runtime msg $msg. No-op for wsmDao.deleteDisk."
            )
            .whenA(diskResourceOpt.isEmpty)
          deleteDisk = diskResourceOpt.traverse { disk =>
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

          networkResourceOpt <- controlledResourceQuery
            .getWsmRecordForRuntime(runtime.id, WsmResourceType.AzureNetwork)
            .transaction
          _ <- logger
            .info(ctx.loggingCtx)(
              s"No network resource found for delete azure runtime msg $msg. No-op for wsmDao.deleteNetworks."
            )
            .whenA(networkResourceOpt.isEmpty)
          deleteNetworks = networkResourceOpt.traverse { network =>
            wsmDao.deleteNetworks(
              DeleteWsmResourceRequest(
                msg.workspaceId,
                network.resourceId,
                DeleteControlledAzureResourceRequest(
                  WsmJobControl(WsmJobId(s"delete-networks-${ctx.traceId.asString.take(10)}"))
                )
              ),
              auth
            )
          }.void

          _ <- List(deleteDisk, deleteNetworks).parSequence
          _ <- dbRef.inTransaction(clusterQuery.updateClusterStatus(runtime.id, RuntimeStatus.Deleted, ctx.now))
          _ <- msg.diskId.traverse(diskId =>
            dbRef.inTransaction(persistentDiskQuery.updateStatus(diskId, DiskStatus.Deleted, ctx.now))
          )
          _ <- logger.info(ctx.loggingCtx)("runtime is deleted successfully")
        } yield ()
        _ <- respOpt match {
          case Some(resp) =>
            resp.jobReport.status match {
              case WsmJobStatus.Succeeded =>
                continue
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
          case None => continue
        }
      } yield ()

      _ <- asyncTasks.offer(
        Task(
          ctx.traceId,
          taskToRun,
          Some(e =>
            dbRef
              .inTransaction(
                clusterErrorQuery
                  .save(runtime.id, RuntimeError(e.getMessage, None, ctx.now, Some(ctx.traceId))) >>
                  clusterQuery.updateClusterStatus(runtime.id, RuntimeStatus.Error, ctx.now)
              )
              .void
          ),
          ctx.now,
          "deleteAzureRuntime"
        )
      )
    } yield ()
  }

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
}
