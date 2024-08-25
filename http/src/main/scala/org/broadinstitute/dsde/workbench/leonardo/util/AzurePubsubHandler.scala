package org.broadinstitute.dsde.workbench
package leonardo
package util

import bio.terra.workspace.api.ControlledAzureResourceApi
import bio.terra.workspace.model.{
  AzureDiskCreationParameters,
  AzureStorageContainerCreationParameters,
  AzureVmCreationParameters,
  AzureVmCustomScriptExtension,
  AzureVmCustomScriptExtensionSetting,
  AzureVmUser,
  AzureVmUserAssignedIdentities,
  CloningInstructionsEnum,
  ControlledResourceCommonFields,
  CreateControlledAzureDiskRequestV2Body,
  CreateControlledAzureResourceResult,
  CreateControlledAzureStorageContainerRequestBody,
  CreateControlledAzureVmRequestBody,
  CreatedControlledAzureVmResult,
  DeleteControlledAzureResourceResult,
  JobControl,
  JobReport
}
import cats.Parallel
import cats.effect.Async
import cats.effect.std.Queue
import cats.mtl.Ask
import cats.syntax.all._
import com.azure.resourcemanager.compute.models.{PowerState, VirtualMachine, VirtualMachineSizeTypes}
import org.broadinstitute.dsde.workbench.azure._
import org.broadinstitute.dsde.workbench.google2.{streamFUntilDone, streamUntilDoneOrTimeout, RegionName}
import org.broadinstitute.dsde.workbench.leonardo.AsyncTaskProcessor.{Task, TaskMetricsTags}
import org.broadinstitute.dsde.workbench.leonardo.SamResourceId.PrivateAzureStorageAccountSamResourceId
import org.broadinstitute.dsde.workbench.leonardo.config.{
  ApplicationConfig,
  AzureEnvironmentConverter,
  ContentSecurityPolicyConfig,
  RefererConfig
}
import org.broadinstitute.dsde.workbench.leonardo.dao._
import org.broadinstitute.dsde.workbench.leonardo.db._
import org.broadinstitute.dsde.workbench.leonardo.http.{ctxConversion, dbioToIO, ConfigReader}
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
import org.typelevel.log4cats.StructuredLogger
import reactor.core.publisher.Mono

import java.time.{Duration, Instant}
import java.util.UUID
import scala.concurrent.ExecutionContext
import scala.jdk.CollectionConverters._

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

  implicit private def wsmDeleteDoneControlledAzureResourceDoneCheckable
    : DoneCheckable[DeleteControlledAzureResourceResult] = (v: DeleteControlledAzureResourceResult) =>
    v.getJobReport.getStatus.equals(JobReport.StatusEnum.SUCCEEDED) || v.getJobReport.getStatus
      .equals(JobReport.StatusEnum.FAILED)

  implicit private def wsmCreateAzureResourceResultDoneCheckable: DoneCheckable[CreateControlledAzureResourceResult] =
    (v: CreateControlledAzureResourceResult) =>
      v.getJobReport.getStatus.equals(JobReport.StatusEnum.SUCCEEDED) || v.getJobReport.getStatus
        .equals(JobReport.StatusEnum.FAILED)

  implicit private def wsmCreateAzureVmResultDoneCheckable: DoneCheckable[CreatedControlledAzureVmResult] =
    (v: CreatedControlledAzureVmResult) =>
      v.getJobReport.getStatus.equals(JobReport.StatusEnum.SUCCEEDED) || v.getJobReport.getStatus
        .equals(JobReport.StatusEnum.FAILED)

  implicit private def vmStopDoneCheckable: DoneCheckable[Option[VirtualMachine]] = (v: Option[VirtualMachine]) =>
    v.get.powerState() == PowerState.DEALLOCATED

  override def createAndPollRuntime(msg: CreateAzureRuntimeMessage)(implicit ev: Ask[F, AppContext]): F[Unit] =
    for {
      ctx <- ev.ask
      _ <- logger.info(s"[AzurePubsubHandler/createAndPollRuntime] beginning for runtime ${msg.runtimeId}")
      runtimeOpt <- clusterQuery.getClusterById(msg.runtimeId).transaction
      runtime <- F.fromOption(runtimeOpt, PubsubHandleMessageError.ClusterNotFound(msg.runtimeId, msg))
      runtimeConfig <- RuntimeConfigQueries.getRuntimeConfig(runtime.runtimeConfigId).transaction
      azureConfig <- runtimeConfig match {
        case x: RuntimeConfig.AzureConfig => F.pure(x)
        case x =>
          F.raiseError[RuntimeConfig.AzureConfig](
            new RuntimeException(s"this runtime doesn't have proper azure config $x")
          )
      }

      // verify the cloud context
      cloudContext = runtime.cloudContext match {
        case _: CloudContext.Gcp =>
          throw PubsubHandleMessageError.ClusterError(runtime.id,
                                                      ctx.traceId,
                                                      "Azure runtime should not have GCP cloud context"
          )
        case cc: CloudContext.Azure => cc
      }

      // Query the Landing Zone service for the landing zone resources
      leoAuth <- samDAO.getLeoAuthToken
      landingZoneResources <- wsmDao.getLandingZoneResources(msg.billingProfileId, leoAuth)

      // Infer the runtime region from the Landing Zone
      _ <- RuntimeConfigQueries
        .updateRegion(runtime.runtimeConfigId, Some(RegionName(landingZoneResources.region.name())))
        .transaction

      _ <- logger.info(
        s"[AzurePubsubHandler/createAndPollRuntime] getting workspace storage container from WSM for runtime ${msg.runtimeId}"
      )
      // Get the optional storage container for the workspace
      workspaceStorageContainerOpt <- wsmDao.getWorkspaceStorageContainer(
          msg.workspaceId,
          leoAuth
        )

      workspaceStorageContainer <- F.fromOption(
        workspaceStorageContainerOpt,
        AzureRuntimeCreationError(
          runtime.id,
          msg.workspaceId,
          s"Storage container not found for workspace: ${msg.workspaceId.value}",
          msg.useExistingDisk
        )
      )

      // send create disk message to WSM
      createDiskResult <- createDiskForRuntime(
        CreateAzureDiskParams(msg.workspaceId, runtime, msg.useExistingDisk, azureConfig)
      )

      // Get optional action managed identity from Sam for the private_azure_storage_account/read action.
      // Identities must be passed to WSM for application-managed resources.
      actionIdentityOpt <- samDAO.getAzureActionManagedIdentity(
          leoAuth,
          PrivateAzureStorageAccountSamResourceId(msg.billingProfileId.value),
          PrivateAzureStorageAccountAction.Read
        )

      _ <- logger.info(
        s"[AzurePubsubHandler/createAndPollRuntime] beginning to monitor runtime creation for runtime ${msg.runtimeId}"
      )

      // all other resources (hybrid connection, storage container, vm)
      // are created within the async task
      _ <- monitorCreateRuntime(
        PollRuntimeParams(
          msg.workspaceId,
          runtime,
          msg.useExistingDisk,
          createDiskResult,
          landingZoneResources,
          azureConfig,
          config.runtimeDefaults.image,
          workspaceStorageContainer,
          msg.workspaceName,
          AzureEnvironmentConverter
            .fromString(ConfigReader.appConfig.azure.hostingModeConfig.azureEnvironment)
            .getStorageEndpointSuffix,
          cloudContext,
          List(actionIdentityOpt).flatten
        )
      )
    } yield ()

  private def setupCreateVmCreateMessage(params: PollRuntimeParams,
                                         storageContainer: CreateStorageContainerResourcesResult,
                                         hcPrimaryKey: PrimaryKey,
                                         createVmJobId: WsmJobId,
                                         hcName: RelayHybridConnectionName
  ): CreateControlledAzureVmRequestBody = {
    val samResourceId = WsmControlledResourceId(UUID.fromString(params.runtime.samResource.resourceId))

    val wsStorageContainerUrl =
      s"https://${params.landingZoneResources.storageAccountName.value}.${params.storageAccountUrlDomain}/${params.workspaceStorageContainer.name.value}"

    // Setup create VM message
    val vmCommon = getCommonFieldsForWsmGeneratedClient(
      ControlledResourceName(params.runtime.runtimeName.asString),
      config.runtimeDefaults.vmControlledResourceDesc,
      params.runtime.auditInfo.creator
    )
      .resourceId(samResourceId.value)

    val arguments = List(
      params.landingZoneResources.relayNamespace.value,
      hcName.value,
      "localhost",
      hcPrimaryKey.value,
      config.runtimeDefaults.listenerImage,
      config.samUrl.renderString,
      samResourceId.value.toString,
      "csp.txt",
      config.wsmUrl.renderString,
      params.workspaceId.value.toString,
      params.workspaceStorageContainer.resourceId.value.toString,
      config.welderImage,
      params.runtime.auditInfo.creator.value,
      storageContainer.containerName.value,
      storageContainer.resourceId.value.toString,
      params.workspaceName,
      wsStorageContainerUrl,
      applicationConfig.leoUrlBase,
      params.runtime.runtimeName.asString,
      s"'${refererConfig.validHosts.mkString("','")}'"
    )

    val cmdToExecute = s"touch /var/log/azure_vm_init_script.log && chmod 400 /var/log/azure_vm_init_script.log &&" +
      s"echo \"${contentSecurityPolicyConfig.asString}\" > csp.txt && bash azure_vm_init_script.sh ${arguments
          .map(s => s"'$s'")
          .mkString(" ")} > /var/log/azure_vm_init_script.log"

    val protectedSettings: List[AzureVmCustomScriptExtensionSetting] = List(
      new AzureVmCustomScriptExtensionSetting()
        .key("fileUris")
        .value(config.runtimeDefaults.customScriptExtension.fileUris.asJava),
      new AzureVmCustomScriptExtensionSetting()
        .key("commandToExecute")
        .value(cmdToExecute)
    )

    val customScriptExtension = new AzureVmCustomScriptExtension()
      .name(config.runtimeDefaults.customScriptExtension.name)
      .`type`(config.runtimeDefaults.customScriptExtension.`type`)
      .publisher(config.runtimeDefaults.customScriptExtension.publisher)
      .version(config.runtimeDefaults.customScriptExtension.version)
      .minorVersionAutoUpgrade(config.runtimeDefaults.customScriptExtension.minorVersionAutoUpgrade)
      .protectedSettings(protectedSettings.asJava)

    val vmPassword = AzurePubsubHandler.getAzureVMSecurePassword(applicationConfig.environment,
                                                                 config.runtimeDefaults.vmCredential.password
    )

    val userAssignedIdentities = new AzureVmUserAssignedIdentities()
    userAssignedIdentities.addAll(params.userAssignedIdentities.asJava)

    val creationParams = new AzureVmCreationParameters()
      .customScriptExtension(customScriptExtension)
      .diskId(params.createDiskResult.resourceId.value)
      .vmImage(config.runtimeDefaults.image.toWsm())
      .vmSize(VirtualMachineSizeTypes.fromString(params.runtimeConfig.machineType.value).toString)
      .name(params.runtime.runtimeName.asString)
      .vmUser(
        new AzureVmUser()
          .name(config.runtimeDefaults.vmCredential.username)
          .password(vmPassword)
      )
      .userAssignedIdentities(userAssignedIdentities)

    new CreateControlledAzureVmRequestBody()
      .azureVm(creationParams)
      .common(vmCommon)
      .jobControl(new JobControl().id(createVmJobId.value))
  }

  private def monitorStartRuntime(runtime: Runtime, startVmOp: Option[Mono[Void]])(implicit
    ev: Ask[F, AppContext]
  ): F[Unit] = for {
    ctx <- ev.ask
    task = for {
      _ <- startVmOp.traverse(startVmOp => F.blocking(startVmOp.block(Duration.ofMinutes(5))))
      isJupyterUp = jupyterDAO.isProxyAvailable(runtime.cloudContext, runtime.runtimeName)
      _ <- streamUntilDoneOrTimeout(
        isJupyterUp,
        config.startStopVmPollConfig.maxAttempts,
        config.startStopVmPollConfig.interval,
        s"Jupyter was not running within ${config.createVmPollConfig.maxAttempts} attempts with ${config.createVmPollConfig.interval} delay"
      )
      _ <- clusterQuery.updateClusterStatus(runtime.id, RuntimeStatus.Running, ctx.now).transaction
      _ <- logger.info(ctx.loggingCtx)("runtime is ready")
    } yield ()
    _ <- asyncTasks.offer(
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
        TaskMetricsTags("startRuntimeV2", None, Some(false), CloudProvider.Azure)
      )
    )
  } yield ()

  override def startAndMonitorRuntime(runtime: Runtime, azureCloudContext: AzureCloudContext)(implicit
    ev: Ask[F, AppContext]
  ): F[Unit] = for {
    ctx <- ev.ask
    vm <- azureVmServiceInterp.getAzureVm(InstanceName(runtime.runtimeName.asString), azureCloudContext)

    // if vm is stopping, stopped or deallocated --> send start message and monitor
    // if vm is starting --> monitor
    // if vm is running --> update DB
    // else --> error
    _ <- vm match {
      case Some(vm) =>
        vm.powerState() match {
          case PowerState.STOPPED | PowerState.DEALLOCATED | PowerState.STOPPING | PowerState.DEALLOCATING =>
            for {
              startVmOpOpt <- azureVmServiceInterp.startAzureVm(InstanceName(runtime.runtimeName.asString),
                                                                azureCloudContext
              )
              _ <- startVmOpOpt match {
                case None =>
                  F.raiseError[Unit](
                    AzureRuntimeStartingError(
                      runtime.id,
                      s"Starting runtime ${runtime.id} request to Azure failed.",
                      ctx.traceId
                    )
                  )
                case Some(startVmOp) =>
                  monitorStartRuntime(runtime, Some(startVmOp))
              }
            } yield ()
          case PowerState.STARTING => monitorStartRuntime(runtime, None)
          case PowerState.RUNNING =>
            for {
              _ <- logger.info(s"Runtime ${runtime.runtimeName.asString} already running, no-op for startRuntime")
              _ <- clusterQuery.updateClusterStatus(runtime.id, RuntimeStatus.Running, ctx.now).transaction
            } yield ()
          case _ =>
            F.raiseError(
              AzureRuntimeStartingError(
                runtime.id,
                s"Runtime ${runtime.runtimeName.asString} cannot be started in a ${vm.powerState().toString} state, starting runtime request failed",
                ctx.traceId
              )
            )
        }
      case None =>
        F.raiseError(
          AzureRuntimeStartingError(
            runtime.id,
            s"Runtime ${runtime.runtimeName.asString} cannot be found in Azure, starting runtime request failed",
            ctx.traceId
          )
        )
    }

  } yield ()

  private def monitorStopRuntime(runtime: Runtime, azureCloudContext: AzureCloudContext, monoOpt: Option[Mono[Void]])(
    implicit ev: Ask[F, AppContext]
  ): F[Unit] = for {
    ctx <- ev.ask
    task = for {
      _ <- monoOpt.traverse(mono => F.blocking(mono.block(Duration.ofMinutes(5))))
      vmStopped = azureVmServiceInterp.getAzureVm(InstanceName(runtime.runtimeName.asString), azureCloudContext)
      _ <- streamUntilDoneOrTimeout(
        vmStopped,
        config.startStopVmPollConfig.maxAttempts,
        config.startStopVmPollConfig.interval,
        s"The VM was not stopped within ${config.createVmPollConfig.maxAttempts} attempts with ${config.createVmPollConfig.interval} delay"
      )
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
    _ <- asyncTasks.offer(
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
        TaskMetricsTags("startRuntimeV2", None, Some(false), CloudProvider.Azure)
      )
    )
  } yield ()

  override def stopAndMonitorRuntime(runtime: Runtime, azureCloudContext: AzureCloudContext)(implicit
    ev: Ask[F, AppContext]
  ): F[Unit] = for {
    ctx <- ev.ask
    vm <- azureVmServiceInterp.getAzureVm(InstanceName(runtime.runtimeName.asString), azureCloudContext)

    // if vm is starting/running --> send stop message and monitor
    // if vm is stopping/deallocating --> monitor
    // if vm is stopped/deallocated --> update DB
    // else --> error
    _ <- vm match {
      case Some(vm) =>
        vm.powerState() match {
          case PowerState.STARTING | PowerState.RUNNING =>
            for {

              monoOpt <- azureVmServiceInterp.stopAzureVm(InstanceName(runtime.runtimeName.asString), azureCloudContext)
              _ <- monoOpt match {
                case None =>
                  F.raiseError[Unit](
                    AzureRuntimeStoppingError(
                      runtime.id,
                      s"Stopping runtime ${runtime.id} request to Azure failed.",
                      ctx.traceId
                    )
                  )
                case Some(mono) =>
                  monitorStopRuntime(runtime, azureCloudContext, Some(mono))
              }
            } yield ()
          case PowerState.DEALLOCATING | PowerState.STOPPING => monitorStopRuntime(runtime, azureCloudContext, None)

          case PowerState.DEALLOCATED | PowerState.STOPPED =>
            for {
              _ <- logger.info(s"Runtime ${runtime.runtimeName.asString} already stopped, no-op for stopRuntime")
              _ <- clusterQuery.updateClusterStatus(runtime.id, RuntimeStatus.Stopped, ctx.now).transaction
            } yield ()
          case _ =>
            F.raiseError(
              AzureRuntimeStoppingError(
                runtime.id,
                s"Runtime ${runtime.runtimeName.asString} cannot be stopped in a ${vm.powerState().toString} state, stopping runtime request failed",
                ctx.traceId
              )
            )
        }
      case None =>
        F.raiseError(
          AzureRuntimeStoppingError(
            runtime.id,
            s"Runtime ${runtime.runtimeName.asString} cannot be found in Azure, stopping runtime request failed",
            ctx.traceId
          )
        )
    }
  } yield ()

  private def createStorageContainer(runtime: Runtime,
                                     landingZoneResources: LandingZoneResources,
                                     workspaceId: WorkspaceId
  )(implicit
    ev: Ask[F, AppContext]
  ): F[CreateStorageContainerResourcesResult] = {
    val storageContainerName = ContainerName(s"ls-${runtime.runtimeName.asString}")
    for {
      ctx <- ev.ask[AppContext]
      wsmApi <- buildWsmControlledResourceApiClient
      common = getCommonFieldsForWsmGeneratedClient(ControlledResourceName(storageContainerName.value),
                                                    "leonardo staging bucket",
                                                    runtime.auditInfo.creator
      )
      azureStorageContainer = new AzureStorageContainerCreationParameters()
        .storageContainerName(storageContainerName.value)

      request = new CreateControlledAzureStorageContainerRequestBody()
        .common(common)
        .azureStorageContainer(azureStorageContainer)
      storageContainer <- F
        .delay(wsmApi.createAzureStorageContainer(request, workspaceId.value))
      resourceId = WsmControlledResourceId(storageContainer.getResourceId)
      _ <- controlledResourceQuery
        .save(runtime.id, resourceId, WsmResourceType.AzureStorageContainer)
        .transaction
      _ <- clusterQuery
        .updateStagingBucket(runtime.id, Some(StagingBucket.Azure(storageContainerName)), ctx.now)
        .transaction
    } yield CreateStorageContainerResourcesResult(storageContainerName, resourceId)
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
                                      s"Disk $diskId not found",
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

          _ <- logger.info(
            s"[AzurePubsubHandler/createAndPollRuntime] calling createAzureDiskV2 on WSM for runtime ${params.runtime.id}"
          )
          wsmApi <- buildWsmControlledResourceApiClient
          _ <- F.delay(wsmApi.createAzureDiskV2(request, params.workspaceId.value))

          syncDiskResp = CreateDiskForRuntimeResult(
            WsmControlledResourceId(common.getResourceId),
            Some(
              PollDiskParams(params.workspaceId,
                             createDiskJobId,
                             disk.id,
                             params.runtime,
                             WsmControlledResourceId(common.getResourceId)
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

      _ <- resp.getJobReport.getStatus match {
        case JobReport.StatusEnum.FAILED =>
          logger.error(s"Wsm createDisk job failed due to ${resp.getErrorReport.getMessage}") >> F.raiseError[Unit](
            AzureRuntimeCreationError(
              params.runtime.id,
              params.workspaceId,
              s"Wsm createDisk job failed due to ${resp.getErrorReport.getMessage}",
              useExistingDisk = false
            )
          )
        case JobReport.StatusEnum.RUNNING =>
          F.raiseError[Unit](
            AzureRuntimeCreationError(
              params.runtime.id,
              params.workspaceId,
              s"Wsm createDisk job was not completed within ${config.createDiskPollConfig.maxAttempts} attempts with ${config.createDiskPollConfig.interval} delay",
              useExistingDisk = false
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

  // we expect the caller of this to update the disk status, since this is monitoring for the WSM record deletion
  // this is important not to conflate the two, since a Wsm resource can exist without a leo resource and vice versa (if the systems get into a bad state)
  private def monitorDeleteDisk(params: PollDeleteDiskParams)(implicit
    ev: Ask[F, AppContext]
  ): F[Unit] =
    for {
      ctx <- ev.ask
      wsmApi <- buildWsmControlledResourceApiClient
      getWsmJobResult = F.delay(wsmApi.getDeleteAzureDiskResult(params.workspaceId.value, params.jobId.value))

      _ <- F.sleep(
        config.deleteDiskPollConfig.initialDelay
      )

      resp <- streamFUntilDone(
        getWsmJobResult,
        config.deleteDiskPollConfig.maxAttempts,
        config.deleteDiskPollConfig.interval
      ).compile.lastOrError

      _ <- resp.getJobReport.getStatus match {
        case JobReport.StatusEnum.SUCCEEDED =>
          for {
            _ <- logger.info(ctx.loggingCtx)(
              s"disk  for runtime ${params.runtime.id} with resource id ${params.wsmResourceId} is deleted successfully"
            )
          } yield ()
        case JobReport.StatusEnum.FAILED =>
          for {
            _ <- logger.error(s"Wsm deleteDisk job failed due to ${resp.getErrorReport.getMessage}")
            _ <- params.diskId.traverse(id =>
              dbRef.inTransaction(persistentDiskQuery.updateStatus(id, DiskStatus.Error, ctx.now))
            )
            _ <- F.raiseError[Unit](
              AzureRuntimeDeletionError(
                params.runtime.id,
                params.diskId,
                params.workspaceId,
                s"Wsm deleteDisk job failed due to ${resp.getErrorReport.getMessage}, disk resource id ${params.wsmResourceId}"
              )
            )
          } yield ()
        case JobReport.StatusEnum.RUNNING =>
          params.diskId.traverse(id =>
            dbRef.inTransaction(persistentDiskQuery.updateStatus(id, DiskStatus.Error, ctx.now))
          ) >> F.raiseError[Unit](
            AzureRuntimeDeletionError(
              params.runtime.id,
              params.diskId,
              params.workspaceId,
              s"Wsm deleteDisk was not completed within ${config.deleteDiskPollConfig.maxAttempts} attempts with ${config.deleteDiskPollConfig.interval} delay, disk resource id ${params.wsmResourceId}"
            )
          )
      }
    } yield ()

  private def monitorDeleteVm(params: PollVmParams)(implicit
    ev: Ask[F, AppContext]
  ): F[Unit] =
    for {
      ctx <- ev.ask
      wsmApi <- buildWsmControlledResourceApiClient
      getWsmJobResult = F.delay(wsmApi.getDeleteAzureVmResult(params.workspaceId.value, params.jobId.value))

      _ <- F.sleep(
        config.deleteVmPollConfig.initialDelay
      )

      resp <- streamFUntilDone(
        getWsmJobResult,
        config.deleteVmPollConfig.maxAttempts,
        config.deleteVmPollConfig.interval
      ).compile.lastOrError

      _ <- resp.getJobReport.getStatus match {
        case JobReport.StatusEnum.SUCCEEDED =>
          for {
            _ <- logger.info(ctx.loggingCtx)(
              s"vm ${params.runtime.id} is deleted successfully"
            )
          } yield ()
        case JobReport.StatusEnum.FAILED =>
          logger.error(s"Wsm deleteVm job failed due to ${resp.getErrorReport.getMessage}") >> F.raiseError[Unit](
            AzureRuntimeDeletionError(
              params.runtime.id,
              params.diskId,
              params.workspaceId,
              s"WSM delete VM job failed due to ${resp.getErrorReport.getMessage}"
            )
          )
        case JobReport.StatusEnum.RUNNING =>
          F.raiseError[Unit](
            AzureRuntimeDeletionError(
              params.runtime.id,
              params.diskId,
              params.workspaceId,
              s"WSM delete VM job was not completed within ${config.deleteVmPollConfig.maxAttempts} attempts with ${config.deleteVmPollConfig.interval} delay"
            )
          )
      }
    } yield ()

  private def monitorDeleteStorageContainer(params: PollStorageContainerParams)(implicit
    ev: Ask[F, AppContext]
  ): F[Unit] =
    for {
      ctx <- ev.ask
      wsmApi <- buildWsmControlledResourceApiClient
      getWsmJobResult = F.delay(
        wsmApi.getDeleteAzureStorageContainerResult(params.workspaceId.value, params.jobId.value)
      )

      _ <- F.sleep(
        config.deleteStorageContainerPollConfig.initialDelay
      )

      // it does not take long to delete a storage container
      resp <- streamFUntilDone(
        getWsmJobResult,
        config.deleteStorageContainerPollConfig.maxAttempts,
        config.deleteStorageContainerPollConfig.interval
      ).compile.lastOrError

      _ <- resp.getJobReport.getStatus match {
        case JobReport.StatusEnum.SUCCEEDED =>
          for {
            _ <- logger.info(ctx.loggingCtx)(
              s"storage container for runtime ${params.runtime.id} is deleted successfully"
            )
          } yield ()
        case JobReport.StatusEnum.FAILED =>
          logger.error(s"Wsm deleteStorageContainer job failed due to ${resp.getErrorReport.getMessage}") >> F
            .raiseError[Unit](
              AzureRuntimeDeletionError(
                params.runtime.id,
                params.diskId,
                params.workspaceId,
                s"WSM storage container delete job failed due to ${resp.getErrorReport.getMessage} for runtime ${params.runtime.id}"
              )
            )
        case JobReport.StatusEnum.RUNNING =>
          F.raiseError[Unit](
            AzureRuntimeDeletionError(
              params.runtime.id,
              params.diskId,
              params.workspaceId,
              s"WSM delete storage container job was not completed within ${config.deleteStorageContainerPollConfig.maxAttempts} attempts with ${config.deleteStorageContainerPollConfig.interval} delay"
            )
          )
      }
    } yield ()
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

  private def monitorCreateRuntime(params: PollRuntimeParams)(implicit ev: Ask[F, AppContext]): F[Unit] = {
    val hybridConnectionName = RelayHybridConnectionName(params.runtime.runtimeName.asString)

    for {
      ctx <- ev.ask
      createVmJobId = WsmJobId(s"create-vm-${params.runtime.id.toString.take(10)}")
      wsmControlledResourceClient <- buildWsmControlledResourceApiClient
      getWsmVmJobResult = F.delay(
        wsmControlledResourceClient.getCreateAzureVmResult(params.workspaceId.value, createVmJobId.value)
      )

      wsmApi <- buildWsmControlledResourceApiClient

      isJupyterUp = jupyterDAO.isProxyAvailable(params.cloudContext, params.runtime.runtimeName)
      isWelderUp = welderDao.isProxyAvailable(params.cloudContext, params.runtime.runtimeName)

      taskToRun = for {
        // the result of creating the hybrid connection + storage container are necessary for the createVm request
        hcPrimaryKey <- azureRelay.createRelayHybridConnection(params.landingZoneResources.relayNamespace,
                                                               hybridConnectionName,
                                                               params.cloudContext.value
        )
        storageContainer <- createStorageContainer(params.runtime, params.landingZoneResources, params.workspaceId)

        _ <- params.createDiskResult.pollParams.traverse(params => monitorCreateDisk(params))

        vmRequest = setupCreateVmCreateMessage(
          params,
          storageContainer,
          hcPrimaryKey,
          createVmJobId,
          hybridConnectionName
        )

        _ <- F.delay(wsmApi.createAzureVm(vmRequest, params.workspaceId.value))

        // it takes a while to create Azure VM. Hence sleep sometime before we start polling WSM
        _ <- F.sleep(
          config.createVmPollConfig.initialDelay
        )

        // Poll the WSM createVm job for completion
        resp <- streamFUntilDone(
          getWsmVmJobResult,
          config.createVmPollConfig.maxAttempts,
          config.createVmPollConfig.interval
        ).compile.lastOrError
        _ <- resp.getJobReport.getStatus match {
          case JobReport.StatusEnum.FAILED =>
            F.raiseError[Unit](
              AzureRuntimeCreationError(
                params.runtime.id,
                params.workspaceId,
                s"Wsm createVm job failed due to ${resp.getErrorReport.getMessage}",
                params.useExistingDisk
              )
            )
          case JobReport.StatusEnum.RUNNING =>
            F.raiseError[Unit](
              AzureRuntimeCreationError(
                params.runtime.id,
                params.workspaceId,
                s"Wsm createVm job was not completed within ${config.createVmPollConfig.maxAttempts} attempts with ${config.createVmPollConfig.interval} delay",
                params.useExistingDisk
              )
            )
          case JobReport.StatusEnum.SUCCEEDED =>
            val hostIp = s"${params.landingZoneResources.relayNamespace.value}.servicebus.windows.net"
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
              unsanitizedRegion = resp.getAzureVm.getAttributes.getRegion
              region = if (unsanitizedRegion == null) None else Some(RegionName(unsanitizedRegion))
              // Update runtime region to the VM region
              _ <- RuntimeConfigQueries
                .updateRegion(params.runtime.runtimeConfigId, region)
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
          TaskMetricsTags("createRuntimeV2", None, Some(false), CloudProvider.Azure)
        )
      )
    } yield ()
  }

  override def deleteAndPollRuntime(msg: DeleteAzureRuntimeMessage)(implicit ev: Ask[F, AppContext]): F[Unit] = {
    for {
      // Perform initial db retrievals
      ctx <- ev.ask

      runtimeOpt <- dbRef.inTransaction(clusterQuery.getClusterById(msg.runtimeId))
      runtime <- F.fromOption(runtimeOpt, PubsubHandleMessageError.ClusterNotFound(msg.runtimeId, msg))
      auth <- samDAO.getLeoAuthToken
      wsmApi <- buildWsmControlledResourceApiClient

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

      // Delete the storage container in WSM if it exists
      storageContainerResourceOpt <- controlledResourceQuery
        .getWsmRecordForRuntime(runtime.id, WsmResourceType.AzureStorageContainer)
        .transaction

      deleteStorageContainerActionOpt = storageContainerResourceOpt.fold {
        // if the storage container hasn't been created in WSM yet, skip storage container deletion
        logger
          .info(
            s"No storage container resource found for runtime ${msg.runtimeId.toString} in ${msg.workspaceId.value}. No-op for wsmApi.deleteAzureStorageContainer."
          )
          .map(_ => none[PollStorageContainerParams])
      } { storageContainerResourceRecord =>
        val deleteJobControl = getDeleteControlledResourceRequest()
        F
          .delay(
            wsmApi.deleteAzureStorageContainer(deleteJobControl,
                                               msg.workspaceId.value,
                                               storageContainerResourceRecord.resourceId.value
            )
          )
          .map(_ =>
            Some(
              PollStorageContainerParams(msg.workspaceId,
                                         WsmJobId(deleteJobControl.getJobControl.getId),
                                         runtime,
                                         msg.diskIdToDelete
              )
            )
          )
      }

      // Query the Landing Zone service for the landing zone resources
      landingZoneResources <- wsmDao.getLandingZoneResources(msg.billingProfileId, auth)

      // Delete hybrid connection for this VM, delayed later for async task queue
      deleteHybridConnectionAction = azureRelay.deleteRelayHybridConnection(
        landingZoneResources.relayNamespace,
        RelayHybridConnectionName(runtime.runtimeName.asString),
        cloudContext
      )

      // if there's a disk to delete, find the disk record associated with the runtime
      // - if there's a disk record, then delete in WSM
      //   - update the disk's Leo state if it exists in leo AND the user specifies deletion

      // Perform lookups needed to get wsm disk information from DB
      diskRecordOpt <- msg.diskIdToDelete.flatTraverse { _ =>
        controlledResourceQuery
          .getWsmRecordForRuntime(msg.runtimeId, WsmResourceType.AzureDisk)
          .transaction
      }

      // Formulate the WSM call and polling params for later use in async disk deletion
      wsmDeleteDiskActionOpt = diskRecordOpt.fold {
        // if the disk hasn't been created in WSM yet, skip disk deletion
        logger
          .info(
            s"No disk resource found for runtime ${msg.runtimeId.toString} in workspace ${msg.workspaceId.value}. No-op for wsmApi.deleteAzureDisk."
          )
          .map(_ => none[PollDeleteDiskParams])
      } { wsmDiskRecord =>
        for {
          deleteJobControl <- F.delay(getDeleteControlledResourceRequest())
          _ <- F.delay(
            wsmApi.deleteAzureDisk(deleteJobControl, msg.workspaceId.value, wsmDiskRecord.resourceId.value)
          )
        } yield Some(
          PollDeleteDiskParams(msg.workspaceId,
                               WsmJobId(deleteJobControl.getJobControl.getId),
                               msg.diskIdToDelete,
                               runtime,
                               wsmDiskRecord.resourceId
          )
        )
      }

      // Even if there is no wsm record yet, we may still have a leo disk record to clean up
      leoDiskCleanupActionOpt = msg.diskIdToDelete.traverse { diskId =>
        dbRef.inTransaction(persistentDiskQuery.delete(diskId, ctx.now))
      }

      // Formulate the WSM call and polling params for later use in async VM deletion
      deleteVmActionOpt = msg.wsmResourceId.fold {
        // Error'd runtimes might not have a WSM resourceId. We still want deletion to succeed in this case.
        logger
          .info(ctx.loggingCtx)(
            s"No VM wsmResourceId found for delete azure runtime msg $msg. No-op for wsmApi.deleteAzureVm."
          )
          .map(_ => none[PollVmParams])
      } { resourceId =>
        for {
          deleteJobControl <- F.delay(getDeleteControlledResourceRequest())
          _ <- F.delay(wsmApi.deleteAzureVm(deleteJobControl, msg.workspaceId.value, resourceId.value))
        } yield Some(
          PollVmParams(msg.workspaceId, WsmJobId(deleteJobControl.getJobControl.getId), runtime, msg.diskIdToDelete)
        )
      }

      // Delete and poll on all associated resources in WSM, and then mark the runtime as deleted
      // The resources handled here are Storage container, Hybrid connection, Virtual Machine, and Disk
      // It is worth noting disk error handling is a bit special as it has its own table, the polling method for that is responsible for updating the disk to error state
      // A possible optimization is pairing each (wsmCall, pollingOnCompletion) into operations and running them in parallel
      // This would ensure that the ordering in conjunction with a single failure doesn't prevent as many things as possible from being deleted
      taskToRun = for {
        _ <- logger.info(ctx.loggingCtx)(
          s"beginning azure storage container deletion and polling for runtime ${msg.runtimeId} in workspace ${msg.workspaceId}"
        )
        pollStorageContainerParamsOpt <- deleteStorageContainerActionOpt
        _ <- pollStorageContainerParamsOpt.traverse(params => monitorDeleteStorageContainer(params))

        _ <- logger.info(ctx.loggingCtx)(
          s"beginning hybrid connection deletion for runtime ${msg.runtimeId} in workspace ${msg.workspaceId}"
        )
        _ <- deleteHybridConnectionAction

        // Vm must be done before disk
        _ <- logger.info(ctx.loggingCtx)(
          s"beginning azure vm deletion and polling for runtime ${msg.runtimeId} in workspace ${msg.workspaceId}"
        )
        pollDeleteVmParamsOpt <- deleteVmActionOpt
        _ <- pollDeleteVmParamsOpt.traverse(params => monitorDeleteVm(params))

        // if Some(leodisk) None(wsmDisk) -> mark deleted
        // if Some(leodisk) Some(wsmDisk) -> poll then mark deleted
        // if none(leodisk) Some(wsmDisk) -> do not poll, user wants to keep disk, noop
        // if none(LeoDisk) none(wsmDisk) -> noop
        _ <- logger.info(ctx.loggingCtx)(
          s"beginning azure disk deletion and polling for runtime ${msg.runtimeId} in workspace ${msg.workspaceId}"
        )
        pollDiskParamsOpt <-
          if (msg.diskIdToDelete.isDefined) wsmDeleteDiskActionOpt else F.delay(none[PollDeleteDiskParams])
        _ <- pollDiskParamsOpt.traverse(params => monitorDeleteDisk(params))
        _ <- leoDiskCleanupActionOpt

        _ <- dbRef.inTransaction(clusterQuery.completeDeletion(runtime.id, ctx.now))
        _ <- logger.info(ctx.loggingCtx)(
          s"runtime ${msg.runtimeId} with name ${runtime.runtimeName.asString} is deleted successfully"
        )
      } yield ()

      _ <- asyncTasks.offer(
        Task(
          ctx.traceId,
          taskToRun,
          Some { e =>
            handleAzureRuntimeDeletionError(
              AzureRuntimeDeletionError(msg.runtimeId,
                                        msg.diskIdToDelete,
                                        msg.workspaceId,
                                        s"Fail to delete runtime due to ${e.getMessage}"
              )
            )
          },
          ctx.now,
          TaskMetricsTags("deleteRuntimeV2", None, Some(false), CloudProvider.Azure)
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
    _ <- e.diskId match {
      case Some(diskId) => persistentDiskQuery.updateStatus(diskId, DiskStatus.Error, ctx.now).transaction
      case None         => F.unit
    }
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
                  _ <- deleteDiskInWSM(diskId, diskRecord.resourceId, e.workspaceId, Some(e.runtimeId))
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
          isRetryable = false,
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
    aksAlgebra.updateAndPollApp(UpdateAKSAppParams(appId, appName, appChartVersion, workspaceId, cloudContext))

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
          isRetryable = false,
          None,
          None,
          None
        )
      }
    } yield ()

  private def deleteDiskInWSM(diskId: DiskId,
                              wsmResourceId: WsmControlledResourceId,
                              workspaceId: WorkspaceId,
                              runtimeId: Option[Long]
  )(implicit ev: Ask[F, AppContext]): F[Unit] =
    for {
      ctx <- ev.ask
      jobId = getWsmJobId("delete-disk", wsmResourceId)

      _ <- logger.info(ctx.loggingCtx)(s"Sending WSM delete message for disk resource ${wsmResourceId.value}")
      wsmControlledResourceClient <- buildWsmControlledResourceApiClient
      deleteDiskBody = getDeleteControlledResourceRequest(jobId)
      _ <- F
        .delay(wsmControlledResourceClient.deleteAzureDisk(deleteDiskBody, workspaceId.value, wsmResourceId.value))
        .void
        .adaptError(e =>
          AzureDiskDeletionError(
            diskId,
            wsmResourceId,
            workspaceId,
            s"${ctx.traceId.asString} | WSM call to delete disk failed due to ${e.getMessage}. Please retry delete again"
          )
        )
      getDeleteJobResult = F.delay(wsmControlledResourceClient.getDeleteAzureDiskResult(workspaceId.value, jobId.value))

      // We need to wait until WSM deletion job to be done to update the database
      taskToRun = for {
        resp <- streamFUntilDone(
          getDeleteJobResult,
          config.deleteDiskPollConfig.maxAttempts,
          config.deleteDiskPollConfig.interval
        ).compile.lastOrError

        _ <- resp.getJobReport.getStatus match {
          case JobReport.StatusEnum.SUCCEEDED =>
            for {
              _ <- logger.info(ctx.loggingCtx)(s"disk ${diskId.value} is deleted successfully")
              _ <- runtimeId match {
                case Some(runtimeId) => clusterQuery.setDiskDeleted(runtimeId, ctx.now).transaction
                case _               => dbRef.inTransaction(persistentDiskQuery.delete(diskId, ctx.now)).void
              }
            } yield ()
          case JobReport.StatusEnum.FAILED =>
            F.raiseError[Unit](
              AzureDiskDeletionError(
                diskId,
                wsmResourceId,
                workspaceId,
                s"WSM deleteDisk job failed due to ${resp.getErrorReport.getMessage}"
              )
            )
          case JobReport.StatusEnum.RUNNING =>
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
          TaskMetricsTags("deleteDiskV2", None, Some(false), CloudProvider.Azure)
        )
      )
    } yield ()

  override def deleteDisk(msg: DeleteDiskV2Message)(implicit ev: Ask[F, AppContext]): F[Unit] =
    for {
      ctx <- ev.ask

      _ <- msg.wsmResourceId match {
        case Some(wsmResourceId) =>
          deleteDiskInWSM(msg.diskId, wsmResourceId, msg.workspaceId, None)
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
    s"$jobName-${resourceId.value.toString.take(10)}"
  )

  private def buildWsmControlledResourceApiClient(implicit ev: Ask[F, AppContext]): F[ControlledAzureResourceApi] =
    for {
      auth <- samDAO.getLeoAuthToken
      token <- auth.credentials match {
        case org.http4s.Credentials.Token(_, token) => F.pure(token)
        case _ => F.raiseError[String](new RuntimeException("Could not obtain Leo auth token"))
      }
      wsmControlledResourceClient <- wsmClientProvider.getControlledAzureResourceApi(token)
    } yield wsmControlledResourceClient

  private def getDeleteControlledResourceRequest(
    jobId: WsmJobId = WsmJobId(UUID.randomUUID().toString)
  ): bio.terra.workspace.model.DeleteControlledAzureResourceRequest = {
    val jobControl = new JobControl()
      .id(jobId.value)
    val deleteControlledResource = new bio.terra.workspace.model.DeleteControlledAzureResourceRequest()
      .jobControl(jobControl)

    deleteControlledResource
  }
}
