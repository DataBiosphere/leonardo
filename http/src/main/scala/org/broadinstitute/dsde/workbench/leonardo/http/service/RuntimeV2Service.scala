package org.broadinstitute.dsde.workbench.leonardo
package http
package service

import cats.mtl.Ask
import org.broadinstitute.dsde.workbench.leonardo.config.PersistentDiskConfig
import org.broadinstitute.dsde.workbench.model.UserInfo

//TODO: all functions but non-workspace-specific list are currently azure-specific
trait RuntimeV2Service[F[_]] {
  def createRuntime(userInfo: UserInfo,
                    runtimeName: RuntimeName,
                    workspaceId: WorkspaceId,
                    useExistingDisk: Boolean,
                    req: CreateAzureRuntimeRequest
  )(implicit
    as: Ask[F, AppContext]
  ): F[CreateRuntimeResponse]

  def getRuntime(userInfo: UserInfo, runtimeName: RuntimeName, workspaceId: WorkspaceId)(implicit
    as: Ask[F, AppContext]
  ): F[GetRuntimeResponse]

  def startRuntime(userInfo: UserInfo, runtimeName: RuntimeName, workspaceId: WorkspaceId)(implicit
    as: Ask[F, AppContext]
  ): F[Unit]

  def stopRuntime(userInfo: UserInfo, runtimeName: RuntimeName, workspaceId: WorkspaceId)(implicit
    as: Ask[F, AppContext]
  ): F[Unit]

  def updateRuntime(userInfo: UserInfo,
                    runtimeName: RuntimeName,
                    workspaceId: WorkspaceId,
                    req: UpdateAzureRuntimeRequest
  )(implicit
    as: Ask[F, AppContext]
  ): F[Unit]

  def deleteRuntime(userInfo: UserInfo, runtimeName: RuntimeName, workspaceId: WorkspaceId, deleteDisk: Boolean)(
    implicit as: Ask[F, AppContext]
  ): F[Unit]

  def deleteAllRuntimes(userInfo: UserInfo, workspaceId: WorkspaceId, deleteDisk: Boolean)(implicit
    as: Ask[F, AppContext]
  ): F[Unit]

  def listRuntimes(userInfo: UserInfo,
                   workspaceId: Option[WorkspaceId],
                   cloudProvider: Option[CloudProvider],
                   params: Map[String, String]
  )(implicit
    as: Ask[F, AppContext]
  ): F[Vector[ListRuntimeResponse2]]

  def updateDateAccessed(userInfo: UserInfo, workspaceId: WorkspaceId, runtimeName: RuntimeName)(implicit
    as: Ask[F, AppContext]
  ): F[Unit]
}

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
