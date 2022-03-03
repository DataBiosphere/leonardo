package org.broadinstitute.dsde.workbench.leonardo.http
package service

import cats.mtl.Ask
import org.broadinstitute.dsde.workbench.leonardo.config.PersistentDiskConfig
import org.broadinstitute.dsde.workbench.leonardo.{
  AppContext,
  AzureImageUri,
  CreateAzureRuntimeRequest,
  RuntimeName,
  UpdateAzureRuntimeRequest,
  WorkspaceId
}
import org.broadinstitute.dsde.workbench.model.UserInfo

trait AzureService[F[_]] {
  def createRuntime(userInfo: UserInfo,
                    runtimeName: RuntimeName,
                    workspaceId: WorkspaceId,
                    req: CreateAzureRuntimeRequest)(
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
