package org.broadinstitute.dsde.workbench.leonardo.http
package service

import cats.mtl.Ask
import org.broadinstitute.dsde.workbench.leonardo.config.PersistentDiskConfig
import org.broadinstitute.dsde.workbench.leonardo.{
  AppContext,
  AzureImageUri,
  CidrIP,
  CloudProvider,
  CreateAzureRuntimeRequest,
  RuntimeName,
  UpdateAzureRuntimeRequest,
  WorkspaceId,
  WsmJobId
}
import org.broadinstitute.dsde.workbench.model.UserInfo

//TODO: all functions but non-workspace-specific list are currently azure-specific
trait RuntimeV2Service[F[_]] {
  def createRuntime(userInfo: UserInfo,
                    runtimeName: RuntimeName,
                    workspaceId: WorkspaceId,
                    req: CreateAzureRuntimeRequest,
                    createVmJobId: WsmJobId)(
    implicit as: Ask[F, AppContext]
  ): F[Unit]

  def getRuntime(userInfo: UserInfo, runtimeName: RuntimeName, workspaceId: WorkspaceId)(
    implicit as: Ask[F, AppContext]
  ): F[GetRuntimeResponse]

  def updateRuntime(userInfo: UserInfo,
                    runtimeName: RuntimeName,
                    workspaceId: WorkspaceId,
                    req: UpdateAzureRuntimeRequest)(
    implicit as: Ask[F, AppContext]
  ): F[Unit]

  def deleteRuntime(userInfo: UserInfo, runtimeName: RuntimeName, workspaceId: WorkspaceId)(
    implicit as: Ask[F, AppContext]
  ): F[Unit]

  def listRuntimes(userInfo: UserInfo,
                   workspaceId: Option[WorkspaceId],
                   cloudProvider: Option[CloudProvider],
                   params: Map[String, String])(
    implicit as: Ask[F, AppContext]
  ): F[Vector[ListRuntimeResponse2]]
}

final case class AzureServiceConfig(diskConfig: PersistentDiskConfig, runtimeConfig: AzureRuntimeConfig)
final case class AzureRuntimeConfig(imageUri: AzureImageUri, defaultScopes: Set[String])

final case class AzureRuntimeDefaults(ipControlledResourceDesc: String,
                                      ipNamePrefix: String,
                                      networkControlledResourceDesc: String,
                                      networkNamePrefix: String,
                                      subnetNamePrefix: String,
                                      addressSpaceCidr: CidrIP,
                                      subnetAddressCidr: CidrIP,
                                      diskControlledResourceDesc: String,
                                      vmControlledResourceDesc: String)
