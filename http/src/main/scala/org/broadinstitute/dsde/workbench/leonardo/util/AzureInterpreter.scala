package org.broadinstitute.dsde.workbench
package leonardo
package util

import java.util.UUID

import cats.effect.Async
import cats.mtl.Ask
import cats.effect.std.Queue
import cats.syntax.all._
import com.azure.resourcemanager.compute.models.{PowerState, VirtualMachine, VirtualMachineSizeTypes}
import org.broadinstitute.dsde.workbench.google2.streamUntilDoneOrTimeout
import org.broadinstitute.dsde.workbench.leonardo.AsyncTaskProcessor.Task
import org.broadinstitute.dsde.workbench.leonardo.dao.{
  AccessScope,
  AzureDiskName,
  AzureIpName,
  AzureNetworkName,
  AzureSubnetName,
  CloningInstructions,
  ComputeManagerDao,
  ControlledResourceCommonFields,
  ControlledResourceDescription,
  ControlledResourceIamRole,
  ControlledResourceName,
  CreateDiskRequest,
  CreateDiskRequestData,
  CreateDiskResponse,
  CreateIpRequest,
  CreateIpRequestData,
  CreateIpResponse,
  CreateNetworkRequest,
  CreateNetworkRequestData,
  CreateNetworkResponse,
  CreateVmRequest,
  CreateVmRequestData,
  CreateVmResult,
  DeleteVmRequest,
  GetJobResultRequest,
  ManagedBy,
  PrivateResourceUser,
  WsmDao,
  WsmJobControl,
  WsmJobId,
  WsmJobStatus
}
import org.broadinstitute.dsde.workbench.leonardo.db.{
  clusterErrorQuery,
  clusterImageQuery,
  clusterQuery,
  controlledResourceQuery,
  persistentDiskQuery,
  DbReference,
  RuntimeConfigQueries,
  WsmResourceType
}
import org.broadinstitute.dsde.workbench.leonardo.monitor.LeoPubsubMessage.{
  CreateAzureRuntimeMessage,
  DeleteAzureRuntimeMessage
}
import org.broadinstitute.dsde.workbench.leonardo.monitor.{PollMonitorConfig, PubsubHandleMessageError}
import org.broadinstitute.dsde.workbench.model.{IP, WorkbenchEmail}

import scala.concurrent.ExecutionContext

class AzureInterpreter[F[_]](
  config: AzureInterpretorConfig,
  monitorConfig: AzureMonitorConfig,
  asyncTasks: Queue[F, Task[F]],
  wsmDao: WsmDao[F],
  azureComputeManager: ComputeManagerDao[F]
)(implicit val executionContext: ExecutionContext, dbRef: DbReference[F], F: Async[F])
    extends AzureAlgebra[F] {

  override def createAndPollRuntime(msg: CreateAzureRuntimeMessage)(implicit ev: Ask[F, AppContext]): F[Unit] =
    for {
      ctx <- ev.ask
      runtimeOpt <- dbRef.inTransaction(clusterQuery.getClusterById(msg.runtimeId))
      runtime <- F.fromOption(runtimeOpt, PubsubHandleMessageError.ClusterNotFound(msg.runtimeId, msg))

      runtimeConfig <- dbRef.inTransaction(RuntimeConfigQueries.getRuntimeConfig(runtime.runtimeConfigId))

      azureRuntimeConfig <- runtimeConfig match {
        case x: RuntimeConfig.AzureVmConfig => F.pure(x)
        case _ =>
          F.raiseError[RuntimeConfig.AzureVmConfig](
            PubsubHandleMessageError.AzureRuntimeError(
              msg.runtimeId,
              ctx.traceId,
              Some(msg),
              s"createRuntime in AzureInterp should not get a runtime with a non-azure runtime config"
            )
          )
      }

      pdOpt <- dbRef.inTransaction(persistentDiskQuery.getById(azureRuntimeConfig.persistentDiskId))
      pd <- F.fromOption(pdOpt, PubsubHandleMessageError.DiskNotFound(azureRuntimeConfig.persistentDiskId))

      //TODO: should front-leo persist this instead to allow for eventual API param?
      // decide in https://broadworkbench.atlassian.net/browse/IA-3112
      vmImageOpt <- dbRef.inTransaction(clusterImageQuery.get(runtime.id, RuntimeImageType.AzureVm))
      vmImage <- F.fromOption(
        vmImageOpt,
        PubsubHandleMessageError.AzureRuntimeError(
          msg.runtimeId,
          ctx.traceId,
          Some(msg),
          s"createRuntime in AzureInterp should not get a runtime without a CLUSTER_IMAGES table entry that has type RuntimeImageType.AzureVm"
        )
      )

      params = CreateAzureRuntimeParams(msg.workspaceId, runtime, azureRuntimeConfig, pd, vmImage)

      createAzureRuntimeResult <- createRuntime(params)

      wsmWorkspace <- wsmDao.getWorkspace(msg.workspaceId)
      _ <- monitorCreateRuntime(
        PollRuntimeParams(msg.workspaceId,
                          runtime,
                          wsmWorkspace.azureContext.managedResourceGroupName,
                          createAzureRuntimeResult.jobReport.id)
      )
    } yield ()

  /** Creates an Azure VM but doesn't wait for its completion.
   * This includes creation of all child Azure resources (disk, network, ip), and assumes these are created synchronously
   * */
  private def createRuntime(params: CreateAzureRuntimeParams)(implicit ev: Ask[F, AppContext]): F[CreateVmResult] =
    for {
      createIpResp <- createIp(params)
      createDiskResp <- createDisk(params)
      createNetworkResp <- createNetwork(params)

      vmCommon = getCommonFields(ControlledResourceName(params.runtime.runtimeName.asString),
                                 config.vmControlledResourceDesc,
                                 params.runtime.auditInfo.creator)
      vmRequest: CreateVmRequest = CreateVmRequest(
        params.workspaceId,
        vmCommon,
        CreateVmRequestData(
          params.runtime.runtimeName,
          params.runtimeConfig.region,
          VirtualMachineSizeTypes.fromString(params.runtimeConfig.machineType.value),
          params.vmImage,
          createIpResp.resourceId,
          createDiskResp.resourceId,
          createNetworkResp.resourceId
        )
      )
      createVmResp <- wsmDao.createVm(vmRequest)
      _ <- dbRef.inTransaction(
        controlledResourceQuery.save(
          params.runtime.id,
          createVmResp.vm.resourceId,
          WsmResourceType.AzureVm,
          params.pd.name.value
        )
      )
    } yield createVmResp

  private def monitorCreateRuntime(params: PollRuntimeParams)(implicit ev: Ask[F, AppContext]): F[Unit] = {
    implicit val azureRuntimeCreatingDoneCheckable: DoneCheckable[VirtualMachine] = (v: VirtualMachine) =>
      v.powerState().toString.equals(PowerState.RUNNING.toString)
    implicit val wsmCreateVmDoneCheckable: DoneCheckable[CreateVmResult] = (v: CreateVmResult) =>
      v.jobReport.status.equals(WsmJobStatus.Succeeded)
    for {
      ctx <- ev.ask

      getWsmJobResult = wsmDao.getCreateVmJobResult(GetJobResultRequest(params.workspaceId, params.jobId))
      getRuntimeOpt <- azureComputeManager.getAzureVm(params.runtime.runtimeName, params.resourceGroup)

      getRuntime = F.fromOption(
        getRuntimeOpt,
        PubsubHandleMessageError.AzureRuntimeError(params.runtime.id,
                                                   ctx.traceId,
                                                   None,
                                                   "Could not retrieve vm controlled resource for runtime from DB")
      )

      taskToRun = for {
        // first poll the WSM createVm job for completion
        _ <- streamUntilDoneOrTimeout(
          getWsmJobResult,
          monitorConfig.pollStatus.maxAttempts,
          monitorConfig.pollStatus.interval,
          s"Wsm createVm job was not completed within ${monitorConfig.pollStatus.maxAttempts} attempts with ${monitorConfig.pollStatus.interval} delay"
        )
        // then poll the azure VM for Running status, retrieving the final azure representation
        azureRuntime <- streamUntilDoneOrTimeout(
          getRuntime,
          monitorConfig.pollStatus.maxAttempts,
          monitorConfig.pollStatus.interval,
          s"Azure runtime was not running within ${monitorConfig.pollStatus.maxAttempts} attempts with ${monitorConfig.pollStatus.interval} delay"
        )

        // update host ip from azure response and set the runtime to running
        _ <- dbRef.inTransaction(
          clusterQuery.updateClusterHostIp(params.runtime.id,
                                           Some(IP(azureRuntime.getPrimaryPublicIPAddress.ipAddress())),
                                           ctx.now)
        )
        _ <- dbRef.inTransaction(clusterQuery.updateClusterStatus(params.runtime.id, RuntimeStatus.Running, ctx.now))
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
      vmControlledResourceOpt <- dbRef.inTransaction(
        controlledResourceQuery.getResourceTypeForRuntime(msg.runtimeId, WsmResourceType.AzureVm)
      )
      vmControlledResource <- F.fromOption(
        vmControlledResourceOpt,
        PubsubHandleMessageError.AzureRuntimeError(msg.runtimeId,
                                                   ctx.traceId,
                                                   Some(msg),
                                                   "Could not retrieve vm controlled resource for runtime from DB")
      )

      params = DeleteAzureRuntimeParams(msg.workspaceId, runtime, vmControlledResource)

      _ <- wsmDao.deleteVm(
        DeleteVmRequest(
          params.workspaceId,
          params.vmControlledResource.resourceId,
          WsmJobControl(WsmJobId(UUID.fromString(uniqueName("delete-job"))))
        )
      )

      wsmWorkspace <- wsmDao.getWorkspace(msg.workspaceId)
      getDeleteResult = azureComputeManager.getAzureVm(runtime.runtimeName,
                                                       wsmWorkspace.azureContext.managedResourceGroupName)

      taskToRun = for {
        _ <- streamUntilDoneOrTimeout(
          getDeleteResult,
          monitorConfig.pollStatus.maxAttempts,
          monitorConfig.pollStatus.interval,
          s"Azure vm still exists after ${monitorConfig.pollStatus.maxAttempts} attempts with ${monitorConfig.pollStatus.interval} delay"
        )
        _ <- dbRef.inTransaction(clusterQuery.updateClusterStatus(params.runtime.id, RuntimeStatus.Deleted, ctx.now))
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

  private def createIp(params: CreateAzureRuntimeParams)(implicit ev: Ask[F, AppContext]): F[CreateIpResponse] = {
    val common = getCommonFields(ControlledResourceName(uniqueName(config.ipNamePrefix)),
                                 config.ipControlledResourceDesc,
                                 params.runtime.auditInfo.creator)

    val azureIpName = uniqueName(config.ipNamePrefix)

    val request: CreateIpRequest = CreateIpRequest(
      params.workspaceId,
      common,
      CreateIpRequestData(
        AzureIpName(azureIpName),
        params.runtimeConfig.region
      )
    )
    for {
      ipResp <- wsmDao.createIp(request)
      _ <- dbRef.inTransaction(
        controlledResourceQuery.save(params.runtime.id, ipResp.resourceId, WsmResourceType.AzureIp, azureIpName)
      )
      //TODO: update runtime hostIp after WSM return update
    } yield ipResp
  }

  private def createDisk(params: CreateAzureRuntimeParams)(implicit ev: Ask[F, AppContext]): F[CreateDiskResponse] = {
    val common = getCommonFields(ControlledResourceName(params.pd.name.value),
                                 config.diskControlledResourceDesc,
                                 params.runtime.auditInfo.creator)

    val request: CreateDiskRequest = CreateDiskRequest(
      params.workspaceId,
      common,
      CreateDiskRequestData(
        //TODO: AzureDiskName should go away once DiskName is no longer coupled to google2 disk service
        AzureDiskName(params.pd.name.value),
        params.pd.size,
        params.runtimeConfig.region
      )
    )

    for {
      diskResp <- wsmDao.createDisk(request)
      _ <- dbRef.inTransaction(
        controlledResourceQuery
          .save(params.runtime.id, diskResp.resourceId, WsmResourceType.AzureDisk, params.pd.name.value)
      )
    } yield diskResp
  }

  private def createNetwork(
    params: CreateAzureRuntimeParams
  )(implicit ev: Ask[F, AppContext]): F[CreateNetworkResponse] = {
    val networkName = uniqueName(config.networkNamePrefix)
    val common = getCommonFields(ControlledResourceName(networkName),
                                 config.networkControlledResourceDesc,
                                 params.runtime.auditInfo.creator)

    val request: CreateNetworkRequest = CreateNetworkRequest(
      params.workspaceId,
      common,
      CreateNetworkRequestData(
        AzureNetworkName(networkName),
        AzureSubnetName(uniqueName(config.subnetNamePrefix)),
        config.addressSpaceCidr,
        config.subnetAddressCidr,
        params.runtimeConfig.region
      )
    )

    for {
      networkResp <- wsmDao.createNetwork(request)
      _ <- dbRef.inTransaction(
        controlledResourceQuery
          .save(params.runtime.id, networkResp.resourceId, WsmResourceType.AzureNetwork, params.pd.name.value)
      )
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
          //Editor gives user Writer and Reader.
          //TODO: should we restrict this to just reader?
          List(ControlledResourceIamRole.Editor)
        )
      )
    )

  private def uniqueName(prefix: String): String = prefix + "-" + UUID.randomUUID.toString
}

final case class AzureInterpretorConfig(ipControlledResourceDesc: String,
                                        ipNamePrefix: String,
                                        networkControlledResourceDesc: String,
                                        networkNamePrefix: String,
                                        subnetNamePrefix: String,
                                        addressSpaceCidr: CidrIP,
                                        subnetAddressCidr: CidrIP,
                                        diskControlledResourceDesc: String,
                                        vmControlledResourceDesc: String)

final case class AzureMonitorConfig(pollStatus: PollMonitorConfig)
