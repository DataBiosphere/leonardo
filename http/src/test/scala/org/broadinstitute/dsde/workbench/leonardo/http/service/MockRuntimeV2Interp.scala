package org.broadinstitute.dsde.workbench.leonardo
package http
package service

import cats.effect.IO
import cats.mtl.Ask
import com.azure.resourcemanager.compute.models.VirtualMachineSizeTypes
import org.broadinstitute.dsde.workbench.google2.MachineTypeName
import org.broadinstitute.dsde.workbench.model.UserInfo
import CommonTestData._

class MockRuntimeV2Interp extends RuntimeV2Service[IO] {
  override def createRuntime(userInfo: UserInfo,
                             runtimeName: RuntimeName,
                             workspaceId: WorkspaceId,
                             useExistingDisk: Boolean,
                             req: CreateAzureRuntimeRequest
  )(implicit as: Ask[IO, AppContext]): IO[CreateRuntimeResponse] = for {
    ctx <- as.ask[AppContext]
  } yield CreateRuntimeResponse(ctx.traceId)

  override def getRuntime(userInfo: UserInfo, runtimeName: RuntimeName, workspaceId: WorkspaceId)(implicit
    as: Ask[IO, AppContext]
  ): IO[GetRuntimeResponse] =
    IO.pure(
      GetRuntimeResponse
        .fromRuntime(CommonTestData.testCluster,
                     RuntimeConfig.AzureConfig(MachineTypeName(VirtualMachineSizeTypes.STANDARD_A0.toString),
                                               Some(DiskId(-1)),
                                               azureRegion
                     ),
                     None
        )
        .copy(clusterName = RuntimeName("azureruntime1"))
    )

  override def updateRuntime(userInfo: UserInfo,
                             runtimeName: RuntimeName,
                             workspaceId: WorkspaceId,
                             req: UpdateAzureRuntimeRequest
  )(implicit as: Ask[IO, AppContext]): IO[Unit] = IO.pure()

  override def deleteRuntime(userInfo: UserInfo,
                             runtimeName: RuntimeName,
                             workspaceId: WorkspaceId,
                             deleteDisk: Boolean
  )(implicit
    as: Ask[IO, AppContext]
  ): IO[Unit] = IO.pure()

  override def deleteAllRuntimes(userInfo: UserInfo, workspaceId: WorkspaceId, deleteDisk: Boolean)(implicit
    as: Ask[IO, AppContext]
  ): IO[Unit] = IO.pure()

  override def listRuntimes(
    userInfo: UserInfo,
    workspaceId: Option[WorkspaceId],
    cloudProvider: Option[CloudProvider],
    params: Map[String, String]
  )(implicit as: Ask[IO, AppContext]): IO[Vector[ListRuntimeResponse2]] =
    IO.pure(
      Vector(
        ListRuntimeResponse2(
          CommonTestData.testCluster.id,
          Some(CommonTestData.workspaceId),
          CommonTestData.testCluster.samResource,
          RuntimeName("azureruntime1"),
          CloudContext.Azure(azureCloudContext),
          CommonTestData.testCluster.auditInfo,
          RuntimeConfig.AzureConfig(MachineTypeName(VirtualMachineSizeTypes.STANDARD_A0.toString),
                                    Some(DiskId(-1)),
                                    azureRegion
          ),
          CommonTestData.testCluster.proxyUrl,
          CommonTestData.testCluster.status,
          CommonTestData.testCluster.labels,
          CommonTestData.testCluster.patchInProgress
        )
      )
    )

  override def startRuntime(userInfo: UserInfo, runtimeName: RuntimeName, workspaceId: WorkspaceId)(implicit
    as: Ask[IO, AppContext]
  ): IO[Unit] = IO.unit

  override def stopRuntime(userInfo: UserInfo, runtimeName: RuntimeName, workspaceId: WorkspaceId)(implicit
    as: Ask[IO, AppContext]
  ): IO[Unit] = IO.unit

  override def updateDateAccessed(userInfo: UserInfo, workspaceId: WorkspaceId, runtimeName: RuntimeName)(implicit
    as: Ask[IO, AppContext]
  ): IO[Unit] = IO.unit
}
