package org.broadinstitute.dsde.workbench.leonardo
package dao

import java.util.UUID
import cats.effect.IO
import cats.mtl.Ask
import org.http4s.headers.Authorization

import java.time.ZonedDateTime

class MockWsmDAO(jobStatus: WsmJobStatus = WsmJobStatus.Succeeded) extends WsmDao[IO] {
  override def createIp(request: CreateIpRequest,
                        authorization: Authorization)(implicit ev: Ask[IO, AppContext]): IO[CreateIpResponse] =
    IO.pure(
      CreateIpResponse(
        WsmControlledResourceId(UUID.randomUUID())
      )
    )

  override def createDisk(request: CreateDiskRequest,
                          authorization: Authorization)(implicit ev: Ask[IO, AppContext]): IO[CreateDiskResponse] =
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

  override def createVm(request: CreateVmRequest,
                        authorization: Authorization)(implicit ev: Ask[IO, AppContext]): IO[CreateVmResult] =
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

  override def deleteVm(request: DeleteWsmResourceRequest,
                        authorization: Authorization)(implicit ev: Ask[IO, AppContext]): IO[DeleteWsmResourceResult] =
    IO.pure(
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

  override def getWorkspace(workspaceId: WorkspaceId,
                            authorization: Authorization)(implicit ev: Ask[IO, AppContext]): IO[WorkspaceDescription] =
    IO.pure(
      WorkspaceDescription(
        workspaceId,
        "workspaceName",
        AzureCloudContext(
          TenantId("testTenantId"),
          SubscriptionId("testSubscriptionId"),
          ManagedResourceGroupName("testResourceGroup")
        )
      )
    )

  override def deleteDisk(request: DeleteWsmResourceRequest, authorization: Authorization)(
    implicit ev: Ask[IO, AppContext]
  ): IO[DeleteWsmResourceResult] = IO.pure(
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

  override def deleteIp(request: DeleteWsmResourceRequest, authorization: Authorization)(
    implicit ev: Ask[IO, AppContext]
  ): IO[DeleteWsmResourceResult] = IO.pure(
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

  override def deleteNetworks(request: DeleteWsmResourceRequest, authorization: Authorization)(
    implicit ev: Ask[IO, AppContext]
  ): IO[DeleteWsmResourceResult] = IO.pure(
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
}
