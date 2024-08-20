package org.broadinstitute.dsde.workbench.leonardo
package dao

import cats.effect.IO
import cats.mtl.Ask
import org.broadinstitute.dsde.workbench.azure._
import org.broadinstitute.dsde.workbench.google2.RegionName
import org.http4s.headers.Authorization

import java.time.ZonedDateTime
import java.util.UUID

class MockWsmDAO(jobStatus: WsmJobStatus = WsmJobStatus.Succeeded) extends WsmDao[IO] {

  override def getCreateVmJobResult(
    request: GetJobResultRequest,
    authorization: Authorization
  )(implicit ev: Ask[IO, AppContext]): IO[GetCreateVmJobResult] =
    IO.pure(
      GetCreateVmJobResult(
        Some(
          WsmVm(WsmVMMetadata(WsmControlledResourceId(UUID.randomUUID())),
                WsmVMAttributes(RegionName("southcentralus"))
          )
        ),
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

  override def getLandingZoneResources(billingProfileId: BillingProfileId, userToken: Authorization)(implicit
    ev: Ask[IO, AppContext]
  ): IO[LandingZoneResources] =
    IO.pure(
      CommonTestData.landingZoneResources
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

  override def getDeleteDiskJobResult(request: GetJobResultRequest, authorization: Authorization)(implicit
    ev: Ask[IO, AppContext]
  ): IO[GetDeleteJobResult] = IO.pure(
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

  override def getWorkspaceStorageContainer(workspaceId: WorkspaceId, authorization: Authorization)(implicit
    ev: Ask[IO, AppContext]
  ): IO[Option[StorageContainerResponse]] =
    IO.pure(Some(StorageContainerResponse(ContainerName("dummy"), WsmControlledResourceId(UUID.randomUUID()))))
}
