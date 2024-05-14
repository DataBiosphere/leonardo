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

import java.security.SecureRandom
import java.time.Instant
import scala.util.Random

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
object AzurePubsubHandler {
  private[util] def generateAzureVMSecurePassword(): String = {
    // Azure is enforcing the following constraints for password generation
    // Passwords must not include reserved words or unsupported characters.
    // Password must have 3 of the following: 1 lower case character, 1 upper case character, 1 number, and 1 special character that is not '\'or '-'.
    // The value must be between 12 and 123 characters long.

    val lowerLetters = 'a' to 'z'
    val upperLetters = 'A' to 'Z'
    val numbers = '0' to '9'
    val specialChars = IndexedSeq('!', '@', '#', '$', '&', '*', '?', '^', '(', ')')
    val fullCharset = lowerLetters ++ upperLetters ++ numbers ++ specialChars

    val random = new SecureRandom()

    def pickRandomChars(charSet: IndexedSeq[Char], size: Int): List[Char] =
      Iterator
        .continually(charSet(random.nextInt(charSet.length)))
        .take(size)
        .toList

    val passwordChars: List[Char] = pickRandomChars(lowerLetters, 1) ++
      pickRandomChars(upperLetters, 1) ++
      pickRandomChars(numbers, 1) ++
      pickRandomChars(specialChars, 1) ++
      pickRandomChars(fullCharset, 12)

    Random.shuffle(passwordChars).mkString
  }

  private[util] def getAzureVMSecurePassword(environment: String, sharedPassword: String): String =
    // Generate random password for Azure VM in production, for the other lower level envs we can used the shared password
    environment match {
      case "prod" => generateAzureVMSecurePassword()
      case _      => sharedPassword
    }
}
