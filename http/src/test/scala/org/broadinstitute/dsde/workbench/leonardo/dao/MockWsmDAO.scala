package org.broadinstitute.dsde.workbench.leonardo
package dao

import java.util.UUID
import cats.effect.IO
import cats.mtl.Ask
import com.azure.core.management.Region
import org.broadinstitute.dsde.workbench.azure.{
  AKSClusterName,
  AzureCloudContext,
  ContainerName,
  ManagedResourceGroupName,
  RelayNamespace,
  SubscriptionId,
  TenantId
}
import org.broadinstitute.dsde.workbench.google2.{NetworkName, SubnetworkName}
import org.http4s.headers.Authorization

import java.time.ZonedDateTime

class MockWsmDAO(jobStatus: WsmJobStatus = WsmJobStatus.Succeeded) extends WsmDao[IO] {
  override def createIp(request: CreateIpRequest, authorization: Authorization)(implicit
    ev: Ask[IO, AppContext]
  ): IO[CreateIpResponse] =
    IO.pure(
      CreateIpResponse(
        WsmControlledResourceId(UUID.randomUUID())
      )
    )

  override def createDisk(request: CreateDiskRequest, authorization: Authorization)(implicit
    ev: Ask[IO, AppContext]
  ): IO[CreateDiskResponse] =
    IO.pure(
      CreateDiskResponse(
        WsmControlledResourceId(UUID.randomUUID())
      )
    )

  override def createNetwork(
    request: CreateNetworkRequest,
    authorization: Authorization
  )(implicit ev: Ask[IO, AppContext]): IO[CreateNetworkResponse] =
    IO.pure(
      CreateNetworkResponse(
        WsmControlledResourceId(UUID.randomUUID())
      )
    )

  override def createVm(request: CreateVmRequest, authorization: Authorization)(implicit
    ev: Ask[IO, AppContext]
  ): IO[CreateVmResult] =
    IO.pure(
      CreateVmResult(
        WsmJobReport(
          WsmJobId("job1"),
          "desc",
          jobStatus,
          200,
          ZonedDateTime.parse("2022-03-18T15:02:29.264756Z"),
          Some(ZonedDateTime.parse("2022-03-18T15:02:29.264756Z")),
          "resultUrl"
        ),
        if (jobStatus.equals(WsmJobStatus.Failed))
          Some(
            WsmErrorReport(
              "error",
              500,
              List.empty
            )
          )
        else None
      )
    )

  override def getCreateVmJobResult(
    request: GetJobResultRequest,
    authorization: Authorization
  )(implicit ev: Ask[IO, AppContext]): IO[GetCreateVmJobResult] =
    IO.pure(
      GetCreateVmJobResult(
        Some(WsmVm(WsmVMMetadata(WsmControlledResourceId(UUID.randomUUID())))),
        WsmJobReport(
          request.jobId,
          "desc",
          jobStatus,
          200,
          ZonedDateTime.parse("2022-03-18T15:02:29.264756Z"),
          Some(ZonedDateTime.parse("2022-03-18T15:02:29.264756Z")),
          "resultUrl"
        ),
        if (jobStatus.equals(WsmJobStatus.Failed))
          Some(
            WsmErrorReport(
              "error",
              500,
              List.empty
            )
          )
        else None
      )
    )

  override def deleteVm(request: DeleteWsmResourceRequest, authorization: Authorization)(implicit
    ev: Ask[IO, AppContext]
  ): IO[Option[DeleteWsmResourceResult]] =
    IO.pure(
      Some(
        DeleteWsmResourceResult(
          WsmJobReport(
            request.deleteRequest.jobControl.id,
            "desc",
            jobStatus,
            200,
            ZonedDateTime.parse("2022-03-18T15:02:29.264756Z"),
            Some(ZonedDateTime.parse("2022-03-18T15:02:29.264756Z")),
            "resultUrl"
          ),
          if (jobStatus.equals(WsmJobStatus.Failed))
            Some(
              WsmErrorReport(
                "error",
                500,
                List.empty
              )
            )
          else None
        )
      )
    )

  override def getWorkspace(workspaceId: WorkspaceId, authorization: Authorization)(implicit
    ev: Ask[IO, AppContext]
  ): IO[Option[WorkspaceDescription]] =
    IO.pure(
      Some(
        WorkspaceDescription(
          workspaceId,
          "workspaceName" + workspaceId,
          "9f3434cb-8f18-4595-95a9-d9b1ec9731d4",
          Some(
            AzureCloudContext(TenantId(workspaceId.toString),
                              SubscriptionId(workspaceId.toString),
                              ManagedResourceGroupName(workspaceId.toString)
            )
          ),
          None
        )
      )
    )

  override def getLandingZoneResources(billingProfileId: String, userToken: Authorization)(implicit
    ev: Ask[IO, AppContext]
  ): IO[LandingZoneResources] =
    IO.pure(
      LandingZoneResources(
        AKSClusterName("lzcluster"),
        BatchAccountName("lzbatch"),
        RelayNamespace("lznamespace"),
        StorageAccountName("lzstorage"),
        NetworkName("lzvnet"),
        SubnetworkName("batchsub"),
        SubnetworkName("akssub"),
        SubnetworkName("computesub")
      )
    )

  override def deleteDisk(request: DeleteWsmResourceRequest, authorization: Authorization)(implicit
    ev: Ask[IO, AppContext]
  ): IO[Option[DeleteWsmResourceResult]] =
    IO.pure(
      Some(
        DeleteWsmResourceResult(
          WsmJobReport(
            request.deleteRequest.jobControl.id,
            "desc",
            jobStatus,
            200,
            ZonedDateTime.parse("2022-03-18T15:02:29.264756Z"),
            Some(ZonedDateTime.parse("2022-03-18T15:02:29.264756Z")),
            "resultUrl"
          ),
          if (jobStatus.equals(WsmJobStatus.Failed))
            Some(
              WsmErrorReport(
                "error",
                500,
                List.empty
              )
            )
          else None
        )
      )
    )

  override def deleteIp(request: DeleteWsmResourceRequest, authorization: Authorization)(implicit
    ev: Ask[IO, AppContext]
  ): IO[Option[DeleteWsmResourceResult]] =
    IO.pure(
      Some(
        DeleteWsmResourceResult(
          WsmJobReport(
            request.deleteRequest.jobControl.id,
            "desc",
            jobStatus,
            200,
            ZonedDateTime.parse("2022-03-18T15:02:29.264756Z"),
            Some(ZonedDateTime.parse("2022-03-18T15:02:29.264756Z")),
            "resultUrl"
          ),
          if (jobStatus.equals(WsmJobStatus.Failed))
            Some(
              WsmErrorReport(
                "error",
                500,
                List.empty
              )
            )
          else None
        )
      )
    )

  override def deleteNetworks(request: DeleteWsmResourceRequest, authorization: Authorization)(implicit
    ev: Ask[IO, AppContext]
  ): IO[Option[DeleteWsmResourceResult]] = IO.pure(
    Some(
      DeleteWsmResourceResult(
        WsmJobReport(
          request.deleteRequest.jobControl.id,
          "desc",
          jobStatus,
          200,
          ZonedDateTime.parse("2022-03-18T15:02:29.264756Z"),
          Some(ZonedDateTime.parse("2022-03-18T15:02:29.264756Z")),
          "resultUrl"
        ),
        if (jobStatus.equals(WsmJobStatus.Failed))
          Some(
            WsmErrorReport(
              "error",
              500,
              List.empty
            )
          )
        else None
      )
    )
  )

  override def getRelayNamespace(workspaceId: WorkspaceId, region: Region, authorization: Authorization)(implicit
    ev: Ask[IO, AppContext]
  ): IO[Option[RelayNamespace]] = IO.pure(Some(RelayNamespace("fake-relay-ns")))

  override def getDeleteVmJobResult(request: GetJobResultRequest, authorization: Authorization)(implicit
    ev: Ask[IO, AppContext]
  ): IO[Option[GetDeleteJobResult]] = IO.pure(
    Some(
      GetDeleteJobResult(
        WsmJobReport(
          request.jobId,
          "desc",
          jobStatus,
          200,
          ZonedDateTime.parse("2022-03-18T15:02:29.264756Z"),
          Some(ZonedDateTime.parse("2022-03-18T15:02:29.264756Z")),
          "resultUrl"
        ),
        if (jobStatus.equals(WsmJobStatus.Failed))
          Some(
            WsmErrorReport(
              "error",
              500,
              List.empty
            )
          )
        else None
      )
    )
  )

  override def getWorkspaceStorageContainer(workspaceId: WorkspaceId, authorization: Authorization)(implicit
    ev: Ask[IO, AppContext]
  ): IO[Option[StorageContainerResponse]] =
    IO.pure(Some(StorageContainerResponse(ContainerName("dummy"), WsmControlledResourceId(UUID.randomUUID()))))

  override def createStorageContainer(request: CreateStorageContainerRequest, authorization: Authorization)(implicit
    ev: Ask[IO, AppContext]
  ): IO[CreateStorageContainerResult] =
    IO.pure(CreateStorageContainerResult(WsmControlledResourceId(UUID.randomUUID())))

  override def deleteStorageContainer(request: DeleteWsmResourceRequest, authorization: Authorization)(implicit
    ev: Ask[IO, AppContext]
  ): IO[Option[DeleteWsmResourceResult]] = IO.pure(None)
}
