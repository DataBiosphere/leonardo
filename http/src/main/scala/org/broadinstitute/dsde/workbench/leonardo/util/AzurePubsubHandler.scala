package org.broadinstitute.dsde.workbench
package leonardo
package util

import cats.Parallel
import cats.effect.Async
import cats.effect.std.Queue
import cats.mtl.Ask
import cats.syntax.all._
import com.azure.resourcemanager.compute.models.{PowerState, VirtualMachine}
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
import org.broadinstitute.dsde.workbench.leonardo.http.dbioToIO
import scala.concurrent.ExecutionContext

class AzureInterpreter[F[_]: Parallel](
  config: AzureMonitorConfig,
  asyncTasks: Queue[F, Task[F]],
  wsmDao: WsmDao[F],
  samDAO: SamDAO[F],
  azureComputeManager: ComputeManagerDao[F]
)(implicit val executionContext: ExecutionContext, dbRef: DbReference[F], F: Async[F])
    extends AzureAlgebra[F] {

  override def createAndPollRuntime(msg: CreateAzureRuntimeMessage)(implicit ev: Ask[F, AppContext]): F[Unit] =
    for {
      runtimeOpt <- dbRef.inTransaction(clusterQuery.getClusterById(msg.runtimeId))
      runtime <- F.fromOption(runtimeOpt, PubsubHandleMessageError.ClusterNotFound(msg.runtimeId, msg))
      _ <- monitorCreateRuntime(
        PollRuntimeParams(msg.workspaceId, runtime, msg.jobId)
      )
    } yield ()

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
                )
              }
              // then poll the azure VM for Running status, retrieving the final azure representation
              _ <- streamUntilDoneOrTimeout(
                getRuntime,
                config.createVmPollConfig.maxAttempts,
                config.createVmPollConfig.interval,
                s"Azure runtime was not running within ${config.createVmPollConfig.maxAttempts} attempts with ${config.createVmPollConfig.interval} delay"
              )
              _ <- dbRef.inTransaction(
                clusterQuery.updateClusterStatus(params.runtime.id, RuntimeStatus.Running, ctx.now)
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

      _ <- wsmDao.deleteVm(
        DeleteWsmResourceRequest(
          msg.workspaceId,
          msg.wsmResourceId,
          DeleteControlledAzureResourceRequest(WsmJobControl(WsmJobId(s"delete-${msg.runtimeId}-vm")))
        ),
        auth
      )

      ipResourceId <- getResourceId(msg.runtimeId, WsmResourceType.AzureIp)
      deleteIp = wsmDao.deleteIp(
        DeleteWsmResourceRequest(
          msg.workspaceId,
          ipResourceId,
          DeleteControlledAzureResourceRequest(WsmJobControl(WsmJobId(s"delete-${msg.runtimeId}-ip")))
        ),
        auth
      )

      diskResourceId <- getResourceId(msg.runtimeId, WsmResourceType.AzureDisk)
      deleteDisk = wsmDao.deleteDisk(
        DeleteWsmResourceRequest(
          msg.workspaceId,
          diskResourceId,
          DeleteControlledAzureResourceRequest(WsmJobControl(WsmJobId(s"delete-${msg.runtimeId}-disk")))
        ),
        auth
      )

      networkResourceId <- getResourceId(msg.runtimeId, WsmResourceType.AzureNetwork)
      deleteNetworks = wsmDao.deleteNetworks(
        DeleteWsmResourceRequest(
          msg.workspaceId,
          networkResourceId,
          DeleteControlledAzureResourceRequest(WsmJobControl(WsmJobId(s"delete-${msg.runtimeId}-networks")))
        ),
        auth
      )
      _ <- List(deleteDisk, deleteNetworks, deleteIp).parSequence
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

  private def getResourceId(runtimeId: Long, wsmResourceType: WsmResourceType): F[WsmControlledResourceId] =
    for {
      controlledResourceOpt <- controlledResourceQuery
        .getWsmRecordForRuntime(runtimeId, wsmResourceType)
        .transaction
      azureRuntimeControlledResource <- F.fromOption(
        controlledResourceOpt,
        new Exception(s"WSM resource(${wsmResourceType}) Id is not found ${runtimeId}")
      )
    } yield azureRuntimeControlledResource.resourceId
}

final case class AzureMonitorConfig(createVmPollConfig: PollMonitorConfig, deleteVmPollConfig: PollMonitorConfig)
