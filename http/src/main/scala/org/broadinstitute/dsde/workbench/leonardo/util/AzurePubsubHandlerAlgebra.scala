package org.broadinstitute.dsde.workbench.leonardo
package util

import org.broadinstitute.dsde.workbench.azure.ContainerName
import org.broadinstitute.dsde.workbench.leonardo.WsmControlledResourceId
import org.broadinstitute.dsde.workbench.leonardo.config.PersistentDiskConfig
import org.broadinstitute.dsde.workbench.leonardo.dao.{CreateDiskForRuntimeResult, StorageContainerResponse}
import org.broadinstitute.dsde.workbench.leonardo.monitor.PollMonitorConfig
import org.http4s.Uri


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
                                   storageAccountUrlDomain: String,
                                   cloudContext: CloudContext.Azure,
                                   userAssignedIdentities: List[String]
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

final case class PollVmParams(workspaceId: WorkspaceId, jobId: WsmJobId, runtime: Runtime, diskId: Option[DiskId])

final case class PollStorageContainerParams(workspaceId: WorkspaceId,
                                            jobId: WsmJobId,
                                            runtime: Runtime,
                                            diskId: Option[DiskId]
)

final case class CreateStorageContainerResourcesResult(containerName: ContainerName,
                                                       resourceId: WsmControlledResourceId
)

final case class CustomScriptExtensionConfig(name: String,
                                             publisher: String,
                                             `type`: String,
                                             version: String,
                                             minorVersionAutoUpgrade: Boolean,
                                             fileUris: List[String]
                                            )

final case class AzureServiceConfig(diskConfig: PersistentDiskConfig,
                                    image: AzureImage,
                                    listenerImage: String,
                                    welderImage: String
                                   )
final case class VMCredential(username: String, password: String)

final case class AzureRuntimeDefaults(ipControlledResourceDesc: String,
                                      ipNamePrefix: String,
                                      networkControlledResourceDesc: String,
                                      networkNamePrefix: String,
                                      subnetNamePrefix: String,
                                      addressSpaceCidr: CidrIP,
                                      subnetAddressCidr: CidrIP,
                                      diskControlledResourceDesc: String,
                                      vmControlledResourceDesc: String,
                                      image: AzureImage,
                                      customScriptExtension: CustomScriptExtensionConfig,
                                      listenerImage: String,
                                      vmCredential: VMCredential
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
