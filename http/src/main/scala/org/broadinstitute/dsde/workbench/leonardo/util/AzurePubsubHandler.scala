package org.broadinstitute.dsde.workbench
package leonardo
package util

import cats.Parallel
import cats.effect.Async
import cats.effect.std.Queue
import cats.mtl.Ask
import cats.syntax.all._
import com.azure.resourcemanager.compute.models.{PowerState, VirtualMachine, VirtualMachineSizeTypes}
import org.broadinstitute.dsde.workbench.google2.{streamFUntilDone, streamUntilDoneOrTimeout}
import org.broadinstitute.dsde.workbench.leonardo.AsyncTaskProcessor.Task
import org.broadinstitute.dsde.workbench.leonardo.SamResourceId.WsmResourceSamResourceId
import org.broadinstitute.dsde.workbench.leonardo.dao._
import org.broadinstitute.dsde.workbench.leonardo.db._
import org.broadinstitute.dsde.workbench.leonardo.http.dbioToIO
import org.broadinstitute.dsde.workbench.leonardo.monitor.LeoPubsubMessage.{
  CreateAzureRuntimeMessage,
  DeleteAzureRuntimeMessage
}
import org.broadinstitute.dsde.workbench.leonardo.monitor.PubsubHandleMessageError
import org.broadinstitute.dsde.workbench.leonardo.monitor.PubsubHandleMessageError.AzureRuntimeError
import org.broadinstitute.dsde.workbench.model.{IP, WorkbenchEmail}
import org.http4s.headers.Authorization
import org.typelevel.log4cats.StructuredLogger

import scala.concurrent.ExecutionContext

class AzurePubsubHandlerInterp[F[_]: Parallel](
  config: AzurePubsubHandlerConfig,
  asyncTasks: Queue[F, Task[F]],
  wsmDao: WsmDao[F],
  samDAO: SamDAO[F],
  azureManager: AzureManagerDao[F]
)(implicit val executionContext: ExecutionContext, dbRef: DbReference[F], logger: StructuredLogger[F], F: Async[F])
    extends AzurePubsubHandlerAlgebra[F] {

  override def createAndPollRuntime(msg: CreateAzureRuntimeMessage)(implicit ev: Ask[F, AppContext]): F[Unit] =
    for {
      runtimeOpt <- clusterQuery.getClusterById(msg.runtimeId).transaction
      runtime <- F.fromOption(runtimeOpt, PubsubHandleMessageError.ClusterNotFound(msg.runtimeId, msg))
      runtimeConfig <- RuntimeConfigQueries.getRuntimeConfig(runtime.runtimeConfigId).transaction
      azureConfig <- runtimeConfig match {
        case x: RuntimeConfig.AzureConfig => F.pure(x)
        case x                            => F.raiseError(new RuntimeException(s"this runtime doesn't have proper azure config ${x}"))
      }
      _ <- createRuntime(CreateAzureRuntimeParams(msg.workspaceId,
                                                  runtime,
                                                  msg.relayNamespace,
                                                  azureConfig,
                                                  config.runtimeDefaults.image),
                         WsmJobControl(msg.jobId))
      _ <- monitorCreateRuntime(
        PollRuntimeParams(msg.workspaceId, runtime, msg.jobId, msg.relayNamespace)
      )
    } yield ()

  /** Creates an Azure VM but doesn't wait for its completion.
   * This includes creation of all child Azure resources (disk, network, ip), and assumes these are created synchronously
   * */
  private def createRuntime(params: CreateAzureRuntimeParams,
                            jobControl: WsmJobControl)(implicit ev: Ask[F, AppContext]): F[Unit] =
    for {
      ctx <- ev.ask
      auth <- samDAO.getLeoAuthToken

      cloudContext = params.runtime.cloudContext match {
        case _: CloudContext.Gcp =>
          throw PubsubHandleMessageError.AzureRuntimeError(params.runtime.id,
                                                           ctx.traceId,
                                                           None,
                                                           "Azure runtime should not have GCP cloud context")
        case x: CloudContext.Azure => x
      }

      hcName = RelayHybridConnectionName(params.runtime.runtimeName.asString)
      primaryKey <- azureManager.createRelayHybridConnection(params.relayeNamespace, hcName, cloudContext.value)
      createIpAction = createIp(params, auth, params.runtime.runtimeName.asString)
      createDiskAction = createDisk(params, auth)
      createNetworkAction = createNetwork(params, auth, params.runtime.runtimeName.asString)

      createVmRequest <- (createIpAction, createDiskAction, createNetworkAction).parMapN {
        (ipResp, diskResp, networkResp) =>
          val vmCommon = getCommonFields(ControlledResourceName(params.runtime.runtimeName.asString),
                                         config.runtimeDefaults.vmControlledResourceDesc,
                                         params.runtime.auditInfo.creator)
          val cmdToExecute =
            s"bash azure_vm_init_script.sh ${params.relayeNamespace.value} ${hcName.value} localhost listener ${primaryKey.value} ${config.runtimeDefaults.acrCredential.username} ${config.runtimeDefaults.acrCredential.password} ${config.runtimeDefaults.listenerImage}"
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
              config.runtimeDefaults.acrCredential,
              ipResp.resourceId,
              diskResp.resourceId,
              networkResp.resourceId
            ),
            jobControl
          )
      }
      _ <- wsmDao.createVm(createVmRequest, auth)
    } yield ()

  private def createIp(params: CreateAzureRuntimeParams, leoAuth: Authorization, nameSuffix: String)(
    implicit ev: Ask[F, AppContext]
  ): F[CreateIpResponse] = {
    val common = getCommonFields(ControlledResourceName(s"ip-${nameSuffix}"),
                                 config.runtimeDefaults.ipControlledResourceDesc,
                                 params.runtime.auditInfo.creator)

    val request: CreateIpRequest = CreateIpRequest(
      params.workspaceId,
      common,
      CreateIpRequestData(
        AzureIpName(s"ip-${nameSuffix}"),
        params.runtimeConfig.region
      )
    )
    for {
      ipResp <- wsmDao.createIp(request, leoAuth)
      _ <- controlledResourceQuery.save(params.runtime.id, ipResp.resourceId, WsmResourceType.AzureIp).transaction
    } yield ipResp
  }

  private def createDisk(params: CreateAzureRuntimeParams, leoAuth: Authorization)(
    implicit ev: Ask[F, AppContext]
  ): F[CreateDiskResponse] =
    for {
      ctx <- ev.ask
      diskOpt <- persistentDiskQuery.getById(params.runtimeConfig.persistentDiskId).transaction
      disk <- F.fromOption(diskOpt, new RuntimeException("no disk found"))
      common = getCommonFields(ControlledResourceName(disk.name.value),
                               config.runtimeDefaults.diskControlledResourceDesc,
                               params.runtime.auditInfo.creator)
      request: CreateDiskRequest = CreateDiskRequest(
        params.workspaceId,
        common,
        CreateDiskRequestData(
          //TODO: AzureDiskName should go away once DiskName is no longer coupled to google2 disk service
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
                                 params.runtime.auditInfo.creator)
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

  private def getCommonFields(name: ControlledResourceName, resourceDesc: String, userEmail: WorkbenchEmail) =
    ControlledResourceCommonFields(
      name,
      ControlledResourceDescription(resourceDesc),
      CloningInstructions.Nothing, //TODO: these resources will not be cloned with clone-workspace. Is this correct?
      AccessScope.PrivateAccess,
      ManagedBy.Application,
      Some(
        PrivateResourceUser(
          userEmail,
          List(ControlledResourceIamRole.Writer)
        )
      )
    )

  private def monitorCreateRuntime(params: PollRuntimeParams)(implicit ev: Ask[F, AppContext]): F[Unit] = {
    implicit val azureRuntimeCreatingDoneCheckable: DoneCheckable[VirtualMachine] = (v: VirtualMachine) =>
      v.powerState().toString.equals(PowerState.RUNNING.toString)
    implicit val wsmCreateVmDoneCheckable: DoneCheckable[GetCreateVmJobResult] = (v: GetCreateVmJobResult) =>
      v.jobReport.status.equals(WsmJobStatus.Succeeded) || v.jobReport.status == WsmJobStatus.Failed
    for {
      ctx <- ev.ask

      auth <- samDAO.getLeoAuthToken
      getWsmJobResult = wsmDao.getCreateVmJobResult(GetJobResultRequest(params.workspaceId, params.jobId), auth)

      cloudContext = params.runtime.cloudContext match {
        case _: CloudContext.Gcp =>
          throw PubsubHandleMessageError.AzureRuntimeError(params.runtime.id,
                                                           ctx.traceId,
                                                           None,
                                                           "Azure runtime should not have GCP cloud context")
        case x: CloudContext.Azure => x
      }

      // TODO: this probably isn't super necessary...But we should add a check for pinging jupyter once proxy work is done
      getRuntime = azureManager
        .getAzureVm(params.runtime.runtimeName, cloudContext.value)
        .flatMap(op =>
          F.fromOption(
            op,
            PubsubHandleMessageError
              .AzureRuntimeError(params.runtime.id, ctx.traceId, None, "Could not retrieve vm for runtime from azure")
          )
        )

      taskToRun = for {
        _ <- F.sleep(
          config.createVmPollConfig.initialDelay
        ) //it takes a while to create Azure VM. Hence sleep sometime before we start polling WSM
        // first poll the WSM createVm job for completion
        resp <- streamFUntilDone(
          getWsmJobResult,
          config.createVmPollConfig.maxAttempts,
          config.createVmPollConfig.interval
        ).compile.lastOrError
        _ <- resp.jobReport.status match {
          case WsmJobStatus.Failed =>
            F.raiseError[Unit](
              AzureRuntimeError(
                params.runtime.id,
                ctx.traceId,
                None,
                s"Wsm createVm job failed due due to ${resp.errorReport.map(_.message).getOrElse("unknown")}"
              )
            )
          case WsmJobStatus.Running =>
            F.raiseError[Unit](
              AzureRuntimeError(
                params.runtime.id,
                ctx.traceId,
                None,
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
              // then poll the azure VM for Running status, retrieving the final azure representation
              _ <- streamUntilDoneOrTimeout(
                getRuntime,
                config.createVmPollConfig.maxAttempts,
                config.createVmPollConfig.interval,
                s"Azure runtime was not running within ${config.createVmPollConfig.maxAttempts} attempts with ${config.createVmPollConfig.interval} delay"
              )
              hostIp = s"${params.relayNamespace}.servicebus.windows.net/${params.runtime.runtimeName.asString}"
              _ <- dbRef.inTransaction(
                clusterQuery.setToRunning(params.runtime.id, IP(hostIp), ctx.now)
              )
            } yield ()
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
                  .save(params.runtime.id, RuntimeError(e.getMessage, None, ctx.now, Some(ctx.traceId))) >>
                  clusterQuery.updateClusterStatus(params.runtime.id, RuntimeStatus.Error, ctx.now)
              )
              .void
          ),
          ctx.now
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

      _ <- msg.wsmResourceId.fold(
        logger
          .info(ctx.loggingCtx)(s"No wsmResourceId found for delete azure runtime msg $msg. No-op for wsmDao.deleteVm.")
      ) { wsmResourceId =>
        wsmDao
          .deleteVm(
            DeleteWsmResourceRequest(
              msg.workspaceId,
              wsmResourceId,
              DeleteControlledAzureResourceRequest(WsmJobControl(WsmJobId(s"delete-${msg.runtimeId}-vm")))
            ),
            auth
          )
          .void
      }

      ipResourceOpt <- controlledResourceQuery.getWsmRecordForRuntime(runtime.id, WsmResourceType.AzureIp).transaction
      _ <- logger
        .info(ctx.loggingCtx)(s"No ip resource found for delete azure runtime msg $msg. No-op for wsmDao.deleteIp.")
        .whenA(ipResourceOpt.isEmpty)
      deleteIp = ipResourceOpt.traverse { ip =>
        wsmDao.deleteIp(
          DeleteWsmResourceRequest(
            msg.workspaceId,
            ip.resourceId,
            DeleteControlledAzureResourceRequest(WsmJobControl(WsmJobId(s"delete-${msg.runtimeId}-ip")))
          ),
          auth
        )
      }

      diskResourceOpt <- controlledResourceQuery
        .getWsmRecordForRuntime(runtime.id, WsmResourceType.AzureDisk)
        .transaction
      _ <- logger
        .info(ctx.loggingCtx)(s"No disk resource found for delete azure runtime msg $msg. No-op for wsmDao.deleteDisk.")
        .whenA(diskResourceOpt.isEmpty)
      deleteDisk = diskResourceOpt.traverse { disk =>
        wsmDao.deleteDisk(
          DeleteWsmResourceRequest(
            msg.workspaceId,
            disk.resourceId,
            DeleteControlledAzureResourceRequest(WsmJobControl(WsmJobId(s"delete-${msg.runtimeId}-disk")))
          ),
          auth
        )
      }

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
            DeleteControlledAzureResourceRequest(WsmJobControl(WsmJobId(s"delete-${msg.runtimeId}-networks")))
          ),
          auth
        )
      }
      _ <- List(deleteDisk, deleteNetworks, deleteIp).parSequence
      cloudContext = runtime.cloudContext match {
        case _: CloudContext.Gcp =>
          throw PubsubHandleMessageError.AzureRuntimeError(runtime.id,
                                                           ctx.traceId,
                                                           None,
                                                           "Azure runtime should not have GCP cloud context")
        case x: CloudContext.Azure => x
      }

      getDeleteResult = azureManager.getAzureVm(runtime.runtimeName, cloudContext.value)

      taskToRun = for {
        _ <- streamUntilDoneOrTimeout(
          getDeleteResult,
          config.deleteVmPollConfig.maxAttempts,
          config.deleteVmPollConfig.interval,
          s"Azure vm still exists after ${config.deleteVmPollConfig.maxAttempts} attempts with ${config.deleteVmPollConfig.interval} delay"
        )
        _ <- dbRef.inTransaction(clusterQuery.updateClusterStatus(runtime.id, RuntimeStatus.Deleted, ctx.now))
        _ <- msg.diskId.traverse(diskId =>
          dbRef.inTransaction(persistentDiskQuery.updateStatus(diskId, DiskStatus.Deleted, ctx.now))
        )
        - <- dbRef.inTransaction(controlledResourceQuery.deleteAllForRuntime(runtime.id))
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
          ctx.now
        )
      )
    } yield ()
  }
}
