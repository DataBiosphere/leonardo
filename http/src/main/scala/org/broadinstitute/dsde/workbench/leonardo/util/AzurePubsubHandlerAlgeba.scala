package org.broadinstitute.dsde.workbench.leonardo
package util

import cats.mtl.Ask
import org.broadinstitute.dsde.workbench.azure.RelayNamespace
import org.broadinstitute.dsde.workbench.leonardo.http.service.AzureRuntimeDefaults
import org.broadinstitute.dsde.workbench.leonardo.monitor.LeoPubsubMessage.{
  CreateAzureRuntimeMessage,
  DeleteAzureRuntimeMessage,
  StartRuntimeMessage,
  StopRuntimeMessage
}
import org.broadinstitute.dsde.workbench.leonardo.monitor.PollMonitorConfig
import org.broadinstitute.dsde.workbench.leonardo.monitor.PubsubHandleMessageError.{
  AzureRuntimeCreationError,
  AzureRuntimeDeletionError
}
import org.http4s.Uri

import java.time.Instant

trait AzurePubsubHandlerAlgebra[F[_]] {

  /** Creates an Azure VM but doesn't wait for its completion.
   * This includes creation of all child Azure resources (disk, network, ip), and assumes these are created syncronously
   * */
  def createAndPollRuntime(msg: CreateAzureRuntimeMessage)(implicit ev: Ask[F, AppContext]): F[Unit]

  def deleteAndPollRuntime(msg: DeleteAzureRuntimeMessage)(implicit ev: Ask[F, AppContext]): F[Unit]

  def startAndPollRuntime(msg: StartRuntimeMessage, runtime: Runtime)(implicit ev: Ask[F, AppContext]): F[Unit]

  def stopAndPollRuntime(msg: StopRuntimeMessage)(implicit ev: Ask[F, AppContext]): F[Unit]

  def handleAzureRuntimeCreationError(e: AzureRuntimeCreationError, pubsubMessageSentTime: Instant)(implicit
    ev: Ask[F, AppContext]
  ): F[Unit]

  def handleAzureRuntimeDeletionError(e: AzureRuntimeDeletionError)(implicit
    ev: Ask[F, AppContext]
  ): F[Unit]
}

final case class CreateAzureRuntimeParams(workspaceId: WorkspaceId,
                                          runtime: Runtime,
                                          relayeNamespace: RelayNamespace,
                                          storageContainerResourceId: WsmControlledResourceId,
                                          runtimeConfig: RuntimeConfig.AzureConfig,
                                          vmImage: AzureImage
)
final case class DeleteAzureRuntimeParams(workspaceId: WorkspaceId, runtime: Runtime)

final case class StartAzureRuntimeParams(runtime: Runtime, runtimeConfig: RuntimeConfig.AzureConfig)

final case class PollRuntimeParams(workspaceId: WorkspaceId,
                                   runtime: Runtime,
                                   jobId: WsmJobId,
                                   relayNamespace: RelayNamespace
)

final case class AzurePubsubHandlerConfig(samUrl: Uri,
                                          wsmUrl: Uri,
                                          welderAcrUri: String,
                                          welderImageHash: String,
                                          createVmPollConfig: PollMonitorConfig,
                                          deleteVmPollConfig: PollMonitorConfig,
                                          runtimeDefaults: AzureRuntimeDefaults
) {
  def welderImage: String = s"$welderAcrUri:$welderImageHash"
}
