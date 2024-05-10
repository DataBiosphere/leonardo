package org.broadinstitute.dsde.workbench.leonardo
package util

import cats.mtl.Ask
import org.broadinstitute.dsde.workbench.azure.{AzureCloudContext, ContainerName}
import org.broadinstitute.dsde.workbench.leonardo.WsmControlledResourceId
import org.broadinstitute.dsde.workbench.leonardo.dao.{CreateDiskForRuntimeResult, StorageContainerResponse}
import org.broadinstitute.dsde.workbench.leonardo.http.service.AzureRuntimeDefaults
import org.broadinstitute.dsde.workbench.leonardo.monitor.LeoPubsubMessage.{
  CreateAzureRuntimeMessage,
  DeleteAzureRuntimeMessage,
  DeleteDiskV2Message
}
import org.broadinstitute.dsde.workbench.leonardo.monitor.PollMonitorConfig
import org.broadinstitute.dsde.workbench.leonardo.monitor.PubsubHandleMessageError.{
  AzureRuntimeCreationError,
  AzureRuntimeDeletionError,
  AzureRuntimeStartingError,
  AzureRuntimeStoppingError
}
import org.broadinstitute.dsp.ChartVersion
import org.http4s.Uri

import java.time.Instant

trait AzurePubsubHandlerAlgebra[F[_]] {

  /** Creates an Azure VM but doesn't wait for its completion.
   * This includes creation of all child Azure resources (disk, network, ip), and assumes these are created synchronously
   * */
  def createAndPollRuntime(msg: CreateAzureRuntimeMessage)(implicit ev: Ask[F, AppContext]): F[Unit]

  def deleteAndPollRuntime(msg: DeleteAzureRuntimeMessage)(implicit ev: Ask[F, AppContext]): F[Unit]

  def startAndMonitorRuntime(runtime: Runtime, azureCloudContext: AzureCloudContext)(implicit
    ev: Ask[F, AppContext]
  ): F[Unit]

  def stopAndMonitorRuntime(runtime: Runtime, azureCloudContext: AzureCloudContext)(implicit
    ev: Ask[F, AppContext]
  ): F[Unit]

  def deleteDisk(msg: DeleteDiskV2Message)(implicit ev: Ask[F, AppContext]): F[Unit]

  def createAndPollApp(appId: AppId,
                       appName: AppName,
                       workspaceId: WorkspaceId,
                       cloudContext: AzureCloudContext,
                       billingProfileId: BillingProfileId
  )(implicit
    ev: Ask[F, AppContext]
  ): F[Unit]

  def updateAndPollApp(appId: AppId,
                       appName: AppName,
                       appChartVersion: ChartVersion,
                       workspaceId: Option[WorkspaceId],
                       cloudContext: AzureCloudContext
  )(implicit
    ev: Ask[F, AppContext]
  ): F[Unit]

  def deleteApp(appId: AppId,
                appName: AppName,
                workspaceId: WorkspaceId,
                cloudContext: AzureCloudContext,
                billingProfileId: BillingProfileId
  )(implicit
    ev: Ask[F, AppContext]
  ): F[Unit]

  def handleAzureRuntimeStartError(e: AzureRuntimeStartingError, now: Instant)(implicit
    ev: Ask[F, AppContext]
  ): F[Unit]

  def handleAzureRuntimeStopError(e: AzureRuntimeStoppingError, now: Instant)(implicit
    ev: Ask[F, AppContext]
  ): F[Unit]

  def handleAzureRuntimeCreationError(e: AzureRuntimeCreationError, pubsubMessageSentTime: Instant)(implicit
    ev: Ask[F, AppContext]
  ): F[Unit]

  def handleAzureRuntimeDeletionError(e: AzureRuntimeDeletionError)(implicit
    ev: Ask[F, AppContext]
  ): F[Unit]

  private[util] def generateAzureVMSecurePassword(): String
}

final case class CreateAzureDiskParams(workspaceId: WorkspaceId,
                                       runtime: Runtime,
                                       useExistingDisk: Boolean,
                                       runtimeConfig: RuntimeConfig.AzureConfig
)

/**
 * This case class represents the necessary information to poll all objects associated with the runtime,
 * namely disk, storage container and vm
 */
final case class PollRuntimeParams(workspaceId: WorkspaceId,
                                   runtime: Runtime,
                                   useExistingDisk: Boolean,
                                   createDiskResult: CreateDiskForRuntimeResult,
                                   landingZoneResources: LandingZoneResources,
                                   runtimeConfig: RuntimeConfig.AzureConfig,
                                   vmImage: AzureImage,
                                   workspaceStorageContainer: StorageContainerResponse,
                                   workspaceName: String,
                                   cloudContext: CloudContext.Azure
)

final case class PollDiskParams(workspaceId: WorkspaceId,
                                jobId: WsmJobId,
                                diskId: DiskId,
                                runtime: Runtime,
                                wsmResourceId: WsmControlledResourceId
)

final case class PollDeleteDiskParams(workspaceId: WorkspaceId,
                                      jobId: WsmJobId,
                                      diskId: Option[DiskId],
                                      runtime: Runtime,
                                      wsmResourceId: WsmControlledResourceId
)

final case class PollVmParams(workspaceId: WorkspaceId, jobId: WsmJobId, runtime: Runtime)

final case class PollStorageContainerParams(workspaceId: WorkspaceId, jobId: WsmJobId, runtime: Runtime)

final case class CreateStorageContainerResourcesResult(containerName: ContainerName,
                                                       resourceId: WsmControlledResourceId
)

final case class AzurePubsubHandlerConfig(samUrl: Uri,
                                          wsmUrl: Uri,
                                          welderAcrUri: String,
                                          welderImageHash: String,
                                          createVmPollConfig: PollMonitorConfig,
                                          deleteVmPollConfig: PollMonitorConfig,
                                          startStopVmPollConfig: PollMonitorConfig,
                                          deleteDiskPollConfig: PollMonitorConfig,
                                          runtimeDefaults: AzureRuntimeDefaults,
                                          createDiskPollConfig: PollMonitorConfig,
                                          deleteStorageContainerPollConfig: PollMonitorConfig
) {
  def welderImage: String = s"$welderAcrUri:$welderImageHash"
}
