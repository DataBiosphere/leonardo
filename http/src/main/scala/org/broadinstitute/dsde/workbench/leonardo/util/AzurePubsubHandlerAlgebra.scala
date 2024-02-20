package org.broadinstitute.dsde.workbench.leonardo
package util

import cats.mtl.Ask
import org.broadinstitute.dsde.workbench.azure.{AzureCloudContext, ContainerName, RelayNamespace}
import org.broadinstitute.dsde.workbench.leonardo.dao.{CreateDiskForRuntimeResult, CreateVmRequest}
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
   * This includes creation of all child Azure resources (disk, network, ip), and assumes these are created syncronously
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
}

final case class CreateAzureRuntimeParams(workspaceId: WorkspaceId,
                                          runtime: Runtime,
                                          storageContainerResourceId: WsmControlledResourceId,
                                          landingZoneResources: LandingZoneResources,
                                          runtimeConfig: RuntimeConfig.AzureConfig,
                                          vmImage: AzureImage,
                                          useExistingDisk: Boolean,
                                          workspaceName: String,
                                          storageContainerName: ContainerName
)

final case class CreateAzureDiskParams(workspaceId: WorkspaceId,
                                       runtime: Runtime,
                                       useExistingDisk: Boolean,
                                       runtimeConfig: RuntimeConfig.AzureConfig
)

final case class PollRuntimeParams(workspaceId: WorkspaceId,
                                   runtime: Runtime,
                                   vmJobId: WsmJobId,
                                   relayNamespace: RelayNamespace,
                                   useExistingDisk: Boolean,
                                   runtimeResourcesResult: CreateResourcesRuntimeResult
)

final case class PollDiskParams(workspaceId: WorkspaceId,
                                jobId: WsmJobId,
                                diskId: DiskId,
                                runtime: Runtime,
                                wsmResourceId: WsmControlledResourceId
)

/**
 * This case class represents the necessary information to poll all objects associated with the runtime, namely disk and vm
 */
final case class CreateResourcesRuntimeResult(vmRequest: CreateVmRequest, createDiskResult: CreateDiskForRuntimeResult)

final case class AzurePubsubHandlerConfig(samUrl: Uri,
                                          wsmUrl: Uri,
                                          welderAcrUri: String,
                                          welderImageHash: String,
                                          createVmPollConfig: PollMonitorConfig,
                                          deleteVmPollConfig: PollMonitorConfig,
                                          deleteDiskPollConfig: PollMonitorConfig,
                                          runtimeDefaults: AzureRuntimeDefaults,
                                          createDiskPollConfig: PollMonitorConfig
) {
  def welderImage: String = s"$welderAcrUri:$welderImageHash"
}
