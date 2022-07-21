package org.broadinstitute.dsde.workbench.leonardo
package util

import cats.mtl.Ask
import org.broadinstitute.dsde.workbench.azure.RelayNamespace
import org.broadinstitute.dsde.workbench.leonardo.http.service.AzureRuntimeDefaults
import org.broadinstitute.dsde.workbench.leonardo.monitor.LeoPubsubMessage.{
  CreateAzureRuntimeMessage,
  DeleteAzureRuntimeMessage
}
import org.broadinstitute.dsde.workbench.leonardo.monitor.PollMonitorConfig
import org.broadinstitute.dsde.workbench.leonardo.monitor.PubsubHandleMessageError.AzureRuntimeCreationError
import org.http4s.Uri

import java.time.Instant

trait AzurePubsubHandlerAlgebra[F[_]] {

  /** Creates an Azure VM but doesn't wait for its completion.
   * This includes creation of all child Azure resources (disk, network, ip), and assumes these are created syncronously
   * */
  def createAndPollRuntime(msg: CreateAzureRuntimeMessage)(implicit ev: Ask[F, AppContext]): F[Unit]

  def deleteAndPollRuntime(msg: DeleteAzureRuntimeMessage)(implicit ev: Ask[F, AppContext]): F[Unit]

  def handleAzureRuntimeCreationError(e: AzureRuntimeCreationError, pubsubMessageSentTime: Instant)(implicit
    ev: Ask[F, AppContext]
  ): F[Unit]
}

final case class CreateAzureRuntimeParams(workspaceId: WorkspaceId,
                                          runtime: Runtime,
                                          relayeNamespace: RelayNamespace,
                                          runtimeConfig: RuntimeConfig.AzureConfig,
                                          vmImage: AzureImage
)
final case class DeleteAzureRuntimeParams(workspaceId: WorkspaceId, runtime: Runtime)

final case class PollRuntimeParams(workspaceId: WorkspaceId,
                                   runtime: Runtime,
                                   jobId: WsmJobId,
                                   relayNamespace: RelayNamespace
)

final case class AzurePubsubHandlerConfig(samUrl: Uri,
                                          createVmPollConfig: PollMonitorConfig,
                                          deleteVmPollConfig: PollMonitorConfig,
                                          runtimeDefaults: AzureRuntimeDefaults
)
