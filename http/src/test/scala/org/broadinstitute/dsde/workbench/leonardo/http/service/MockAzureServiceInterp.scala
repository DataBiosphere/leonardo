package org.broadinstitute.dsde.workbench.leonardo
package http
package service

import cats.effect.IO
import cats.mtl.Ask
import com.azure.resourcemanager.compute.models.VirtualMachineSizeTypes
import org.broadinstitute.dsde.workbench.google2.MachineTypeName
import org.broadinstitute.dsde.workbench.model.UserInfo
import CommonTestData._

class MockAzureServiceInterp extends AzureService[IO] {
  override def createRuntime(userInfo: UserInfo,
                             runtimeName: RuntimeName,
                             workspaceId: WorkspaceId,
                             req: CreateAzureRuntimeRequest)(implicit as: Ask[IO, AppContext]): IO[Unit] = IO.pure()

  override def getRuntime(userInfo: UserInfo, runtimeName: RuntimeName, workspaceId: WorkspaceId)(
    implicit as: Ask[IO, AppContext]
  ): IO[GetRuntimeResponse] =
    IO.pure(
      GetRuntimeResponse
        .fromRuntime(CommonTestData.testCluster,
                     RuntimeConfig.AzureConfig(MachineTypeName(VirtualMachineSizeTypes.STANDARD_A0.toString),
                                               DiskId(-1),
                                               azureRegion),
                     None)
        .copy(clusterName = RuntimeName("azureruntime1"))
    )

  override def updateRuntime(userInfo: UserInfo,
                             runtimeName: RuntimeName,
                             workspaceId: WorkspaceId,
                             req: UpdateAzureRuntimeRequest)(implicit as: Ask[IO, AppContext]): IO[Unit] = IO.pure()

  override def deleteRuntime(userInfo: UserInfo, runtimeName: RuntimeName, workspaceId: WorkspaceId)(
    implicit as: Ask[IO, AppContext]
  ): IO[Unit] = IO.pure()
}
