package org.broadinstitute.dsde.workbench.leonardo.http
package service

import cats.mtl.Ask
import org.broadinstitute.dsde.workbench.leonardo.config.PersistentDiskConfig
import org.broadinstitute.dsde.workbench.leonardo.http.api.ListRuntimeResponse2
import org.broadinstitute.dsde.workbench.leonardo.{CreateAzureRuntimeRequest, AzureImageUri, WsmJobId, WorkspaceId, UpdateAzureRuntimeRequest, RuntimeName, AppContext, CidrIP}
import org.broadinstitute.dsde.workbench.model.UserInfo

trait AzureService[F[_]] {
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
