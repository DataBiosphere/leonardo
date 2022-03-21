package org.broadinstitute.dsde.workbench
package leonardo
package util

import cats.effect.Async
import cats.effect.std.Queue
import cats.mtl.Ask
import cats.syntax.all._
import com.azure.resourcemanager.compute.models.{PowerState, VirtualMachine, VirtualMachineSizeTypes}
import org.broadinstitute.dsde.workbench.google2.{streamFUntilDone, streamUntilDoneOrTimeout}
import org.broadinstitute.dsde.workbench.leonardo.AsyncTaskProcessor.Task
import org.broadinstitute.dsde.workbench.leonardo.dao._
import org.broadinstitute.dsde.workbench.leonardo.db._
import org.broadinstitute.dsde.workbench.leonardo.monitor.LeoPubsubMessage.{
  CreateAzureRuntimeMessage,
  DeleteAzureRuntimeMessage
}
import org.broadinstitute.dsde.workbench.leonardo.monitor.PubsubHandleMessageError.AzureRuntimeError
import org.broadinstitute.dsde.workbench.leonardo.monitor.{PollMonitorConfig, PubsubHandleMessageError}
import org.broadinstitute.dsde.workbench.model.WorkbenchEmail
import org.http4s.headers.Authorization

import java.util.UUID
import scala.concurrent.ExecutionContext
import scala.concurrent.duration.DurationInt

class AzureInterpreter[F[_]](
  config: AzureInterpretorConfig,
  monitorConfig: AzureMonitorConfig,
  asyncTasks: Queue[F, Task[F]],
  wsmDao: WsmDao[F],
  samDAO: SamDAO[F],
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
        case x: RuntimeConfig.AzureConfig => F.pure(x)
        case _ =>
          F.raiseError[RuntimeConfig.AzureConfig](
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

      params = CreateAzureRuntimeParams(msg.workspaceId, runtime, azureRuntimeConfig, pd, msg.vmImage)

      jobUUID <- F.delay(UUID.randomUUID()).map(WsmJobId)
      jobControl = WsmJobControl(jobUUID)
      _ <- createRuntime(params, jobControl)

      _ <- monitorCreateRuntime(
        PollRuntimeParams(msg.workspaceId, runtime, jobControl.id, pd)
      )
    } yield ()

  /** Creates an Azure VM but doesn't wait for its completion.
   * This includes creation of all child Azure resources (disk, network, ip), and assumes these are created synchronously
   * */
  private def createRuntime(params: CreateAzureRuntimeParams,
                            jobControl: WsmJobControl)(implicit ev: Ask[F, AppContext]): F[CreateVmResult] =
    for {
      auth <- samDAO.getLeoAuthToken

      createIpResp <- createIp(params, auth, params.runtime.runtimeName.asString)
      createDiskResp <- createDisk(params, auth)
      createNetworkResp <- createNetwork(params, auth, params.runtime.runtimeName.asString)

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
        ),
        jobControl
      )

      createVmResp <- wsmDao.createVm(vmRequest, auth)
    } yield createVmResp
//      CreateVmResult(
//      WsmJobReport(WsmJobId(UUID.randomUUID()), "", WsmJobStatus.Running, 202, ZonedDateTime.now(), None, ""),
//      None
//    )

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

      getRuntime = azureComputeManager
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
          150 seconds
        ) //it takes a while to create Azure VM. Hence sleep sometime before we start polling WSM
        // first poll the WSM createVm job for completion
        resp <- streamFUntilDone(
          getWsmJobResult,
          monitorConfig.pollStatus.maxAttempts,
          monitorConfig.pollStatus.interval
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
                s"Wsm createVm job was not completed within ${monitorConfig.pollStatus.maxAttempts} attempts with ${monitorConfig.pollStatus.interval} delay"
              )
            )
          case WsmJobStatus.Succeeded =>
            for {
              _ <- resp.vm.traverse { x =>
                dbRef.inTransaction(
                  controlledResourceQuery.save(
                    params.runtime.id,
                    x.resourceId,
                    WsmResourceType.AzureVm
                  )
                )
              }
              // then poll the azure VM for Running status, retrieving the final azure representation
              _ <- streamUntilDoneOrTimeout(
                getRuntime,
                monitorConfig.pollStatus.maxAttempts,
                monitorConfig.pollStatus.interval,
                s"Azure runtime was not running within ${monitorConfig.pollStatus.maxAttempts} attempts with ${monitorConfig.pollStatus.interval} delay"
              )
              _ <- dbRef.inTransaction(
                clusterQuery.updateClusterStatus(params.runtime.id, RuntimeStatus.Running, ctx.now)
              )
              _ <- dbRef.inTransaction(persistentDiskQuery.updateStatus(params.disk.id, DiskStatus.Ready, ctx.now))
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

      jobId <- F.delay(UUID.randomUUID()).map(WsmJobId)
      _ <- wsmDao.deleteVm(
        DeleteVmRequest(
          msg.workspaceId,
          msg.wsmResourceId,
          DeleteControlledAzureResourceRequest(WsmJobControl(jobId))
        ),
        auth
      )

      cloudContext = runtime.cloudContext match {
        case _: CloudContext.Gcp =>
          throw PubsubHandleMessageError.AzureRuntimeError(runtime.id,
                                                           ctx.traceId,
                                                           None,
                                                           "Azure runtime should oto have GCP cloud context")
        case x: CloudContext.Azure => x
      }

      getDeleteResult = azureComputeManager.getAzureVm(runtime.runtimeName, cloudContext.value)

      taskToRun = for {
        _ <- streamUntilDoneOrTimeout(
          getDeleteResult,
          monitorConfig.pollStatus.maxAttempts,
          monitorConfig.pollStatus.interval,
          s"Azure vm still exists after ${monitorConfig.pollStatus.maxAttempts} attempts with ${monitorConfig.pollStatus.interval} delay"
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
                  .save(runtime.id, RuntimeError(e.getMessage.take(1024), None, ctx.now, Some(ctx.traceId))) >>
                  clusterQuery.updateClusterStatus(runtime.id, RuntimeStatus.Error, ctx.now)
              )
              .void
          ),
          ctx.now
        )
      )
    } yield ()
  }

  private def createIp(params: CreateAzureRuntimeParams, leoAuth: Authorization, nameSuffix: String)(
    implicit ev: Ask[F, AppContext]
  ): F[CreateIpResponse] = {
    val common = getCommonFields(ControlledResourceName(s"ip-${nameSuffix}"),
                                 config.ipControlledResourceDesc,
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
      _ <- dbRef.inTransaction(
        controlledResourceQuery.save(params.runtime.id, ipResp.resourceId, WsmResourceType.AzureIp)
      )
    } yield ipResp
  }

  private def createDisk(params: CreateAzureRuntimeParams, leoAuth: Authorization)(
    implicit ev: Ask[F, AppContext]
  ): F[CreateDiskResponse] = {
    val common = getCommonFields(ControlledResourceName(params.disk.name.value),
                                 config.diskControlledResourceDesc,
                                 params.runtime.auditInfo.creator)
    val request: CreateDiskRequest = CreateDiskRequest(
      params.workspaceId,
      common,
      CreateDiskRequestData(
        //TODO: AzureDiskName should go away once DiskName is no longer coupled to google2 disk service
        AzureDiskName(params.disk.name.value),
        params.disk.size,
        params.runtimeConfig.region
      )
    )
    for {

      diskResp <- wsmDao.createDisk(request, leoAuth)
      _ <- dbRef.inTransaction(
        controlledResourceQuery
          .save(params.runtime.id, diskResp.resourceId, WsmResourceType.AzureDisk)
      )
    } yield diskResp
  }

  private def createNetwork(
    params: CreateAzureRuntimeParams,
    leoAuth: Authorization,
    nameSuffix: String
  )(implicit ev: Ask[F, AppContext]): F[CreateNetworkResponse] = {
    val common = getCommonFields(ControlledResourceName(s"network-${nameSuffix}"),
                                 config.networkControlledResourceDesc,
                                 params.runtime.auditInfo.creator)
    val request: CreateNetworkRequest = CreateNetworkRequest(
      params.workspaceId,
      common,
      CreateNetworkRequestData(
        AzureNetworkName(s"vNet-${nameSuffix}"),
        AzureSubnetName(s"subnet-${nameSuffix}"),
        config.addressSpaceCidr,
        config.subnetAddressCidr,
        params.runtimeConfig.region
      )
    )
    for {
      networkResp <- wsmDao.createNetwork(request, leoAuth)
      _ <- dbRef.inTransaction(
        controlledResourceQuery
          .save(params.runtime.id, networkResp.resourceId, WsmResourceType.AzureNetwork)
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
          List(ControlledResourceIamRole.Writer)
        )
      )
    )
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
