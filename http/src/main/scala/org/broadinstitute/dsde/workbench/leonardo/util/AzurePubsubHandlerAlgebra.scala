package org.broadinstitute.dsde.workbench.leonardo
package util

import cats.mtl.Ask
import org.broadinstitute.dsde.workbench.azure.{AzureCloudContext, ContainerName}
import org.broadinstitute.dsde.workbench.leonardo.WsmControlledResourceId
import org.broadinstitute.dsde.workbench.leonardo.config.PersistentDiskConfig
import org.broadinstitute.dsde.workbench.leonardo.dao.{CreateDiskForRuntimeResult, StorageContainerResponse}
import org.broadinstitute.dsde.workbench.leonardo.monitor.LeoPubsubMessage.{CreateAzureRuntimeMessage, DeleteAzureRuntimeMessage, DeleteDiskV2Message}
import org.broadinstitute.dsde.workbench.leonardo.monitor.PollMonitorConfig
import org.broadinstitute.dsde.workbench.leonardo.monitor.PubsubHandleMessageError.{AzureRuntimeCreationError, AzureRuntimeDeletionError, AzureRuntimeStartingError, AzureRuntimeStoppingError}
import org.http4s.Uri

import java.security.SecureRandom
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
object AzurePubsubHandler {
  private[util] def generateAzureVMSecurePassword(passwordLength: Int): String = {
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

    var password: String = pickRandomChars(fullCharset, passwordLength).mkString
    // Keep generating passwords until we find one that has all of the required characters
    // This is safer than picking from each subset and then shuffling

    var isPasswordValid: Boolean =
      password.exists(_.isLower) && password.exists(_.isUpper) && password.exists(_.isDigit) && password.exists(
        specialChars.contains
      )

    while (isPasswordValid == false) {
      password = pickRandomChars(fullCharset, passwordLength).mkString
      isPasswordValid =
        password.exists(_.isLower) && password.exists(_.isUpper) && password.exists(_.isDigit) && password.exists(
          specialChars.contains
        )

    }
    password
  }

  private[util] def getAzureVMSecurePassword(environment: String, sharedPassword: String): String =
    // Generate random password for Azure VM in production, for the other lower level envs we can used the shared password
    // The password must be between 12 and 123 characters long. We are choosing 25 here
    environment match {
      case "prod" => generateAzureVMSecurePassword(25)
      case _      => sharedPassword
    }
}
