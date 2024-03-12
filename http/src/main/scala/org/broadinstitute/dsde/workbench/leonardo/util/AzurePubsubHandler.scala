package org.broadinstitute.dsde.workbench
package leonardo
package util

import bio.terra.workspace.api.ControlledAzureResourceApi
import bio.terra.workspace.model.{
  AzureDiskCreationParameters,
  CloningInstructionsEnum,
  ControlledResourceCommonFields,
  CreateControlledAzureDiskRequestV2Body,
  CreateControlledAzureResourceResult,
  JobControl,
  JobReport
}
import cats.Parallel
import cats.effect.Async
import cats.effect.std.Queue
import cats.mtl.Ask
import cats.syntax.all._
import com.azure.resourcemanager.compute.models.VirtualMachineSizeTypes
import org.broadinstitute.dsde.workbench.azure._
import org.broadinstitute.dsde.workbench.google2.{streamFUntilDone, streamUntilDoneOrTimeout, RegionName}
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
import org.http4s.AuthScheme
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
  refererConfig: RefererConfig,
  wsmClientProvider: WsmApiClientProvider[F]
)(implicit val executionContext: ExecutionContext, dbRef: DbReference[F], logger: StructuredLogger[F], F: Async[F])
    extends AzurePubsubHandlerAlgebra[F] {

  // implicits necessary to poll on the status of external jobs
  implicit private def isJupyterUpDoneCheckable: DoneCheckable[Boolean] = (v: Boolean) => v
  implicit private def wsmCreateVmDoneCheckable: DoneCheckable[GetCreateVmJobResult] = (v: GetCreateVmJobResult) =>
    v.jobReport.status.equals(WsmJobStatus.Succeeded) || v.jobReport.status == WsmJobStatus.Failed
  implicit private def wsmDeleteDoneCheckable: DoneCheckable[GetDeleteJobResult] = (v: GetDeleteJobResult) =>
    v.jobReport.status.equals(WsmJobStatus.Succeeded) || v.jobReport.status == WsmJobStatus.Failed

  implicit private def wsmCreateAzureResourceResultDoneCheckable: DoneCheckable[CreateControlledAzureResourceResult] =
    (v: CreateControlledAzureResourceResult) =>
      v.getJobReport().getStatus().equals(JobReport.StatusEnum.SUCCEEDED) || v
        .getJobReport()
        .getStatus()
        .equals(JobReport.StatusEnum.FAILED)

  override def createAndPollRuntime(msg: CreateAzureRuntimeMessage)(implicit ev: Ask[F, AppContext]): F[Unit] =
    for {
      runtimeOpt <- clusterQuery.getClusterById(msg.runtimeId).transaction
      runtime <- F.fromOption(runtimeOpt, PubsubHandleMessageError.ClusterNotFound(msg.runtimeId, msg))
      runtimeConfig <- RuntimeConfigQueries.getRuntimeConfig(runtime.runtimeConfigId).transaction
      azureConfig <- runtimeConfig match {
        case x: RuntimeConfig.AzureConfig => F.pure(x)
        case x => F.raiseError(new RuntimeException(s"this runtime doesn't have proper azure config ${x}"))
      }
      // Query the Landing Zone service for the landing zone resources
      leoAuth <- samDAO.getLeoAuthToken
      landingZoneResources <- wsmDao.getLandingZoneResources(msg.billingProfileId, leoAuth)

      // Infer the runtime region from the Landing Zone
      _ <- RuntimeConfigQueries
        .updateRegion(runtime.runtimeConfigId, Some(RegionName(landingZoneResources.region.name())))
        .transaction

      // Get the optional storage container for the workspace
      tokenOpt <- samDAO.getCachedArbitraryPetAccessToken(runtime.auditInfo.creator)
      storageContainerOpt <- tokenOpt.flatTraverse { token =>
        wsmDao.getWorkspaceStorageContainer(
          msg.workspaceId,
          org.http4s.headers.Authorization(org.http4s.Credentials.Token(AuthScheme.Bearer, token))
        )
      }
      storageContainer <- F.fromOption(
        storageContainerOpt,
        AzureRuntimeCreationError(
          runtime.id,
          msg.workspaceId,
          s"Storage container not found for runtime: ${runtime.id}",
          msg.useExistingDisk
        )
      )
      createVmJobId = WsmJobId(s"create-vm-${runtime.id.toString.take(10)}")

      runtimeResourcesResult <- createRuntime(
        CreateAzureRuntimeParams(
          msg.workspaceId,
          runtime,
          storageContainer.resourceId,
          landingZoneResources,
          azureConfig,
          config.runtimeDefaults.image,
          msg.useExistingDisk,
          msg.workspaceName,
          storageContainer.name
        ),
        WsmJobControl(createVmJobId)
      )
      _ <- monitorCreateRuntime(
        PollRuntimeParams(msg.workspaceId,
                          runtime,
                          createVmJobId,
                          landingZoneResources.relayNamespace,
                          msg.useExistingDisk,
                          runtimeResourcesResult
        )
      )
    } yield ()

  /** Creates an Azure VM but doesn't wait for its completion.
   * This includes creation of all Azure resources excluding disk (network, ip), and assumes these are created synchronously
   * Disk needs to be created before this function is called
   * If useExistingDisk is specified, disk is transferred to new runtime and no new disk is created
   * */
  private def createRuntime(params: CreateAzureRuntimeParams, jobControl: WsmJobControl)(implicit
    ev: Ask[F, AppContext]
  ): F[CreateRuntimeResourcesResult] =
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

      // TODO, run this in parallel to VM creation polling in the async task
      (stagingContainerName, stagingContainerResourceId) <- createStorageContainer(params, auth)

      samResourceId = WsmControlledResourceId(UUID.fromString(params.runtime.samResource.resourceId))
      // Construct the workspace storage container URL which will be passed to the JupyterLab environment variables.
      wsStorageContainerUrl =
        s"https://${params.landingZoneResources.storageAccountName.value}.blob.core.windows.net/${params.storageContainerName.value}"

      createDiskResult <- createDiskForRuntime(
        CreateAzureDiskParams(params.workspaceId, params.runtime, params.useExistingDisk, params.runtimeConfig)
      )

      // Vm logic
      vmCommon = getCommonFields(
        ControlledResourceName(params.runtime.runtimeName.asString),
        config.runtimeDefaults.vmControlledResourceDesc,
        params.runtime.auditInfo.creator,
        Some(samResourceId)
      )
      arguments = List(
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

      cmdToExecute =
        s"touch /var/log/azure_vm_init_script.log && chmod 400 /var/log/azure_vm_init_script.log &&" +
          s"echo \"${contentSecurityPolicyConfig.asString}\" > csp.txt && bash azure_vm_init_script.sh ${arguments
              .map(s => s"'$s'")
              .mkString(" ")} > /var/log/azure_vm_init_script.log"

      createVmRequest = CreateVmRequest(
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
          createDiskResult.resourceId
        ),
        jobControl
      )
    } yield CreateRuntimeResourcesResult(createVmRequest, createDiskResult)

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

  private def createDiskForRuntime(params: CreateAzureDiskParams)(implicit
    ev: Ask[F, AppContext]
  ): F[CreateDiskForRuntimeResult] = for {

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
              s"WSMResource record:${resourceId.value} not found for disk id:${diskId.value}",
              params.useExistingDisk
            )
          )
          // set runtime to new runtimeId for disk
          _ <- controlledResourceQuery
            .updateRuntime(wsmDisk.resourceId, WsmResourceType.AzureDisk, params.runtime.id)
            .transaction
          diskResp = CreateDiskForRuntimeResult(wsmDisk.resourceId, None)
        } yield diskResp

      // if not using existing disk, send a create disk request
      case false =>
        for {
          diskOpt <- persistentDiskQuery.getById(diskId).transaction
          disk <- F.fromOption(
            diskOpt,
            AzureRuntimeCreationError(params.runtime.id,
                                      params.workspaceId,
                                      s"Disk ${diskId} not found",
                                      params.useExistingDisk
            )
          )
          common = getCommonFieldsForWsmGeneratedClient(ControlledResourceName(disk.name.value),
                                                        config.runtimeDefaults.diskControlledResourceDesc,
                                                        params.runtime.auditInfo.creator
          )

          createDiskJobId = WsmJobId(UUID.randomUUID().toString)
          jobControl = new JobControl()
            .id(createDiskJobId.value)

          azureDisk = new AzureDiskCreationParameters()
            .name(disk.name.value)
            .size(disk.size.gb)

          request = new CreateControlledAzureDiskRequestV2Body()
            .common(common)
            .azureDisk(azureDisk)
            .jobControl(jobControl)

          wsmApi <- buildWsmControlledResourceApiClient
          _ <- F.delay(wsmApi.createAzureDiskV2(request, params.workspaceId.value))

          syncDiskResp = CreateDiskForRuntimeResult(
            WsmControlledResourceId(common.getResourceId()),
            Some(
              PollDiskParams(params.workspaceId,
                             createDiskJobId,
                             disk.id,
                             params.runtime,
                             WsmControlledResourceId(common.getResourceId())
              )
            )
          )
        } yield syncDiskResp
    }
  } yield resp

  private def monitorCreateDisk(params: PollDiskParams)(implicit
    ev: Ask[F, AppContext]
  ): F[Unit] =
    for {
      ctx <- ev.ask
      wsmApi <- buildWsmControlledResourceApiClient
      getWsmJobResult = F.delay(wsmApi.getCreateAzureDiskResult(params.workspaceId.value, params.jobId.value))

      _ <- F.sleep(
        config.createDiskPollConfig.initialDelay
      )

      // it does not take long to create a disk
      resp <- streamFUntilDone(
        getWsmJobResult,
        config.createDiskPollConfig.maxAttempts,
        config.createDiskPollConfig.interval
      ).compile.lastOrError

      _ <- resp.getJobReport().getStatus() match {
        case JobReport.StatusEnum.FAILED =>
          F.raiseError[Unit](
            AzureRuntimeCreationError(
              params.runtime.id,
              params.workspaceId,
              s"Wsm createDisk job failed due to ${resp.getErrorReport().getMessage()}",
              false
            )
          )
        case JobReport.StatusEnum.RUNNING =>
          F.raiseError[Unit](
            AzureRuntimeCreationError(
              params.runtime.id,
              params.workspaceId,
              s"Wsm createDisk job was not completed within ${config.createVmPollConfig.maxAttempts} attempts with ${config.createVmPollConfig.interval} delay",
              false
            )
          )
        case JobReport.StatusEnum.SUCCEEDED =>
          for {
            _ <- controlledResourceQuery
              .save(params.runtime.id, params.wsmResourceId, WsmResourceType.AzureDisk)
              .transaction
            _ <- persistentDiskQuery.updateStatus(params.diskId, DiskStatus.Ready, ctx.now).transaction
            _ <- persistentDiskQuery.updateWSMResourceId(params.diskId, params.wsmResourceId, ctx.now).transaction
          } yield ()
      }
    } yield ()

  private def getCommonFields(name: ControlledResourceName,
                              resourceDesc: String,
                              userEmail: WorkbenchEmail,
                              resourceId: Option[WsmControlledResourceId]
  ) =
    InternalDaoControlledResourceCommonFields(
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

  private def getCommonFieldsForWsmGeneratedClient(name: ControlledResourceName,
                                                   resourceDesc: String,
                                                   userEmail: WorkbenchEmail
  ) =
    new ControlledResourceCommonFields()
      .accessScope(bio.terra.workspace.model.AccessScope.PRIVATE_ACCESS)
      .cloningInstructions(CloningInstructionsEnum.NOTHING)
      .description(resourceDesc)
      .name(name.value)
      .managedBy(bio.terra.workspace.model.ManagedBy.APPLICATION)
      .resourceId(UUID.randomUUID())
      .privateResourceUser(
        new bio.terra.workspace.model.PrivateResourceUser()
          .userName(userEmail.value)
          .privateResourceIamRole(bio.terra.workspace.model.ControlledResourceIamRole.WRITER)
      )

  private def monitorCreateRuntime(params: PollRuntimeParams)(implicit ev: Ask[F, AppContext]): F[Unit] =
    for {
      ctx <- ev.ask
      auth <- samDAO.getLeoAuthToken
      getWsmVmJobResult = wsmDao.getCreateVmJobResult(GetJobResultRequest(params.workspaceId, params.vmJobId), auth)

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
        _ <- params.runtimeResourcesResult.createDiskResult.pollParams.traverse(params => monitorCreateDisk(params))

        _ <- wsmDao.createVm(params.runtimeResourcesResult.vmRequest, auth)

        _ <- F.sleep(
          config.createVmPollConfig.initialDelay
        ) // it takes a while to create Azure VM. Hence sleep sometime before we start polling WSM
        // first poll the WSM createVm job for completion

        // VM result
        resp <- streamFUntilDone(
          getWsmVmJobResult,
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
              // Update runtime region to the VM region
              _ <- RuntimeConfigQueries
                .updateRegion(params.runtime.runtimeConfigId, resp.vm.map(_.attributes.region))
                .transaction
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

  override def deleteAndPollRuntime(msg: DeleteAzureRuntimeMessage)(implicit ev: Ask[F, AppContext]): F[Unit] = {
    for {
      ctx <- ev.ask

      runtimeOpt <- dbRef.inTransaction(clusterQuery.getClusterById(msg.runtimeId))
      runtime <- F.fromOption(runtimeOpt, PubsubHandleMessageError.ClusterNotFound(msg.runtimeId, msg))
      auth <- samDAO.getLeoAuthToken

      // Query the Landing Zone service for the landing zone resources
      landingZoneResources <- wsmDao.getLandingZoneResources(msg.billingProfileId, auth)

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
        landingZoneResources.relayNamespace,
        RelayHybridConnectionName(runtime.runtimeName.asString),
        cloudContext
      )

      // if there's a disk to delete, find the disk record associated with the runtime
      // - if there's a disk record, then delete in WSM
      // - update the disk's Leo state
      deleteDiskAction = msg.diskIdToDelete.traverse { diskId =>
        for {
          diskRecordOpt <- controlledResourceQuery
            .getWsmRecordForRuntime(msg.runtimeId, WsmResourceType.AzureDisk)
            .transaction
          _ <- diskRecordOpt match {
            case Some(diskRecord) =>
              for {
                _ <- deleteDiskInWSM(diskId, diskRecord.resourceId, msg.workspaceId, auth, Some(runtime.id))
              } yield ()
            case _ =>
              // if the disk hasn't been created in WSM yet, skip disk deletion
              for {
                _ <- logger.info(
                  s"No disk resource found for runtime ${msg.runtimeId.toString} in ${msg.workspaceId.value}. No-op for wsmDao.deleteDisk."
                )
                _ <- clusterQuery.setDiskDeleted(msg.runtimeId, ctx.now).transaction
              } yield ()
          }
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
                  _ <- logger.info(ctx.loggingCtx)(
                    s"runtime ${msg.runtimeId} is deleted successfully, moving to disk deletion"
                  )
                  _ <- deleteDiskAction
                  _ <- dbRef.inTransaction(clusterQuery.completeDeletion(runtime.id, ctx.now))
                  _ <- logger.info(ctx.loggingCtx)(
                    s"runtime ${msg.runtimeId} ${if (msg.diskIdToDelete.isDefined) "and associated disk"} have been deleted successfully"
                  )
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

  override def handleAzureRuntimeCreationError(e: AzureRuntimeCreationError, now: Instant)(implicit
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
      diskIdOpt <- clusterQuery.getDiskId(e.runtimeId).transaction

      _ <- (e.useExistingDisk, diskIdOpt) match {
        // disk was supposed to be created and was
        case (false, Some(diskId)) =>
          for {
            diskRecordOpt <- controlledResourceQuery
              .getWsmRecordForRuntime(e.runtimeId, WsmResourceType.AzureDisk)
              .transaction
            _ <- diskRecordOpt match {
              // if there is a disk record, the disk finished creating, so it must be deleted in WSM
              case Some(diskRecord) =>
                for {
                  _ <- deleteDiskInWSM(diskId, diskRecord.resourceId, e.workspaceId, auth, Some(e.runtimeId))
                } yield ()
              case _ =>
                for {
                  _ <- logger.info(
                    s"No disk resource found for runtime ${e.runtimeId.toString} in ${e.workspaceId.value}. No-op for wsmDao.deleteDisk."
                  )
                  _ <- clusterQuery.setDiskDeleted(e.runtimeId, now).transaction
                } yield ()
            }
          } yield ()

        // disk was supposed to be created and wasn't
        case (false, None) =>
          logger.info(
            s"No disk resource found for runtime ${e.runtimeId.toString} in ${e.workspaceId.value}. No-op for wsmDao.deleteDisk."
          )
        // no disk created
        case (true, _) => F.unit
      }
    } yield ()

  override def createAndPollApp(appId: AppId,
                                appName: AppName,
                                workspaceId: WorkspaceId,
                                cloudContext: AzureCloudContext,
                                billingProfileId: BillingProfileId
  )(implicit
    ev: Ask[F, AppContext]
  ): F[Unit] =
    for {
      ctx <- ev.ask
      params = CreateAKSAppParams(appId, appName, workspaceId, cloudContext, billingProfileId)
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
    cloudContext: AzureCloudContext,
    billingProfileId: BillingProfileId
  )(implicit
    ev: Ask[F, AppContext]
  ): F[Unit] =
    for {
      ctx <- ev.ask
      params = DeleteAKSAppParams(appName, workspaceId, cloudContext, billingProfileId)
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

  private def deleteDiskInWSM(diskId: DiskId,
                              wsmResourceId: WsmControlledResourceId,
                              workspaceId: WorkspaceId,
                              auth: Authorization,
                              runtimeId: Option[Long]
  )(implicit ev: Ask[F, AppContext]): F[Unit] =
    for {
      ctx <- ev.ask

      jobId = getWsmJobId("delete-disk", wsmResourceId)

      _ <- logger.info(ctx.loggingCtx)(s"Sending WSM delete message for disk resource ${wsmResourceId.value}")
      _ <- wsmDao
        .deleteDisk(
          DeleteWsmResourceRequest(
            workspaceId,
            wsmResourceId,
            DeleteControlledAzureResourceRequest(
              WsmJobControl(jobId)
            )
          ),
          auth
        )
        .void
        .adaptError(e =>
          AzureDiskDeletionError(
            diskId,
            wsmResourceId,
            workspaceId,
            s"${ctx.traceId.asString} | WSM call to delete disk failed due to ${e.getMessage}. Please retry delete again"
          )
        )

      getDeleteJobResult = wsmDao.getDeleteDiskJobResult(
        GetJobResultRequest(workspaceId, jobId),
        auth
      )

      // We need to wait until WSM deletion job to be done to update the database
      taskToRun = for {
        resp <- streamFUntilDone(
          getDeleteJobResult,
          config.deleteDiskPollConfig.maxAttempts,
          config.deleteDiskPollConfig.interval
        ).compile.lastOrError

        _ <- resp.jobReport.status match {
          case WsmJobStatus.Succeeded =>
            for {
              _ <- logger.info(ctx.loggingCtx)(s"disk ${diskId.value} is deleted successfully")
              _ <- runtimeId match {
                case Some(runtimeId) => clusterQuery.setDiskDeleted(runtimeId, ctx.now).transaction
                case _               => dbRef.inTransaction(persistentDiskQuery.delete(diskId, ctx.now))
              }
            } yield ()
          case WsmJobStatus.Failed =>
            F.raiseError[Unit](
              AzureDiskDeletionError(
                diskId,
                wsmResourceId,
                workspaceId,
                s"WSM deleteDisk job failed due to ${resp.errorReport.map(_.message).getOrElse("unknown")}"
              )
            )
          case WsmJobStatus.Running =>
            F.raiseError[Unit](
              AzureDiskDeletionError(
                diskId,
                wsmResourceId,
                workspaceId,
                s"Wsm deleteDisk job was not completed within ${config.deleteDiskPollConfig.maxAttempts} attempts with ${config.deleteDiskPollConfig.interval} delay"
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
              AzureDiskDeletionError(diskId, wsmResourceId, workspaceId, s"Fail to delete disk due to ${e.getMessage}")
            )
          },
          ctx.now,
          "deleteDiskV2"
        )
      )
    } yield ()

  override def deleteDisk(msg: DeleteDiskV2Message)(implicit ev: Ask[F, AppContext]): F[Unit] =
    for {
      ctx <- ev.ask
      auth <- samDAO.getLeoAuthToken

      _ <- msg.wsmResourceId match {
        case Some(wsmResourceId) =>
          deleteDiskInWSM(msg.diskId, wsmResourceId, msg.workspaceId, auth, None)
        case None =>
          for {
            _ <- logger.info(s"No WSM resource found for Azure disk ${msg.diskId}, skipping deletion in WSM")
            _ <- dbRef.inTransaction(persistentDiskQuery.delete(msg.diskId, ctx.now))
            _ <- logger.info(ctx.loggingCtx)(s"disk ${msg.diskId.value} is deleted successfully")
          } yield ()
      }
    } yield ()

  def handleAzureDiskDeletionError(e: AzureDiskDeletionError)(implicit
    ev: Ask[F, AppContext]
  ): F[Unit] = for {
    ctx <- ev.ask
    _ <- logger.error(ctx.loggingCtx, e)(s"Failed to delete Azure disk ${e.diskId}")
    _ <- dbRef.inTransaction(persistentDiskQuery.updateStatus(e.diskId, DiskStatus.Error, ctx.now))
  } yield ()

  def getWsmJobId(jobName: String, resourceId: WsmControlledResourceId): WsmJobId = WsmJobId(
    s"${jobName}-${resourceId.value.toString.take(10)}"
  )

  private def buildWsmControlledResourceApiClient(implicit ev: Ask[F, AppContext]): F[ControlledAzureResourceApi] =
    for {
      auth <- samDAO.getLeoAuthToken
      token <- auth.credentials match {
        case org.http4s.Credentials.Token(_, token) => F.pure(token)
        case _ => F.raiseError(new RuntimeException("Could not obtain Leo auth token"))
      }
      wsmApi <- wsmClientProvider.getControlledAzureResourceApi(token)
    } yield wsmApi

}
