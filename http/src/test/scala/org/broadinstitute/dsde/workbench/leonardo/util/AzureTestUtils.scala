package org.broadinstitute.dsde.workbench.leonardo.util

import scala.collection.mutable
import cats.effect.IO
import org.mockito.Mockito.when
import org.mockito.ArgumentMatchers.any
import bio.terra.workspace.api.{ControlledAzureResourceApi, ResourceApi}
import bio.terra.workspace.model.{
  CreateControlledAzureDiskRequestV2Body,
  CreateControlledAzureResourceResult,
  CreateControlledAzureStorageContainerRequestBody,
  CreatedControlledAzureStorageContainer,
  DeleteControlledAzureResourceRequest,
  DeleteControlledAzureResourceResult,
  ErrorReport,
  JobReport
}
import org.broadinstitute.dsde.workbench.leonardo.CommonTestData.{workspaceId, workspaceId2}
import org.broadinstitute.dsde.workbench.leonardo.dao.WsmApiClientProvider
import org.mockito.ArgumentMatchers
import org.broadinstitute.dsde.workbench.leonardo.dao.{WsmApiClientProvider, WsmDaoDeleteControlledAzureResourceRequest}
import org.scalatestplus.mockito.MockitoSugar

import java.util.UUID

object AzureTestUtils extends MockitoSugar {

  def setUpMockWsmApiClientProvider(
    diskJobStatus: JobReport.StatusEnum = JobReport.StatusEnum.SUCCEEDED,
    vmJobStatus: JobReport.StatusEnum = JobReport.StatusEnum.SUCCEEDED,
    storageContainerJobStatus: JobReport.StatusEnum = JobReport.StatusEnum.SUCCEEDED
  ): (WsmApiClientProvider[IO], ControlledAzureResourceApi, ResourceApi) = {
    val wsm = mock[WsmApiClientProvider[IO]]
    val api = mock[ControlledAzureResourceApi]
    val resourceApi = mock[ResourceApi]
    val disksByJob = mutable.Map.empty[String, CreateControlledAzureDiskRequestV2Body]

    // Create disk
    when {
      api.createAzureDiskV2(any, any)
    } thenAnswer { invocation =>
      val requestBody = invocation.getArgument[CreateControlledAzureDiskRequestV2Body](0)
      val jobId = requestBody.getJobControl.getId
      disksByJob += (jobId -> requestBody)
      new CreateControlledAzureResourceResult()
        .jobReport(
          new JobReport().status(diskJobStatus)
        )
        .errorReport(new ErrorReport())
    }

    // Get disk result
    when {
      api.getCreateAzureDiskResult(any, any)
    } thenAnswer { invocation =>
      val jobId = invocation.getArgument[String](1)
      val requestBody = disksByJob(jobId)
      new CreateControlledAzureResourceResult()
        .jobReport(
          new JobReport().status(diskJobStatus)
        )
        .errorReport(new ErrorReport())
    }

    // Create storage container
    when {
      api.createAzureStorageContainer(any, ArgumentMatchers.eq(workspaceId.value))
    } thenAnswer { invocation =>
      val requestBody = invocation.getArgument[CreateControlledAzureStorageContainerRequestBody](0)
      new CreatedControlledAzureStorageContainer().resourceId(UUID.randomUUID())
    }

    // Create storage container
    when {
      api.createAzureStorageContainer(any, ArgumentMatchers.eq(workspaceId2.value))
    } thenAnswer { _ =>
      throw new Exception("test exception")

    // delete disk
    when {
      api.deleteAzureDisk(any, any, any)
    } thenAnswer { _ =>
      new DeleteControlledAzureResourceResult()
        .jobReport(
          new JobReport().status(diskJobStatus)
        )
        .errorReport(new ErrorReport())
    }

    // delete disk result
    when {
      api.getDeleteAzureDiskResult(any, any)
    } thenAnswer { _ =>
      new DeleteControlledAzureResourceResult()
        .jobReport(
          new JobReport().status(diskJobStatus)
        )
        .errorReport(new ErrorReport())
    }

    // delete vm
    when {
      api.deleteAzureVm(any, any, any)
    } thenAnswer { _ =>
      new DeleteControlledAzureResourceResult()
        .jobReport(
          new JobReport().status(vmJobStatus)
        )
        .errorReport(new ErrorReport())
    }

    // delete vm result
    when {
      api.getDeleteAzureVmResult(any, any)
    } thenAnswer { _ =>
      new DeleteControlledAzureResourceResult()
        .jobReport(
          new JobReport().status(vmJobStatus)
        )
        .errorReport(new ErrorReport())
    }

    // delete storage container
    when {
      api.deleteAzureStorageContainer(any, any, any)
    } thenAnswer { _ =>
      new DeleteControlledAzureResourceResult()
        .jobReport(
          new JobReport().status(storageContainerJobStatus)
        )
        .errorReport(new ErrorReport())
    }

    // delete storage container result
    when {
      api.getDeleteAzureStorageContainerResult(any, any)
    } thenAnswer { _ =>
      new DeleteControlledAzureResourceResult()
        .jobReport(
          new JobReport().status(storageContainerJobStatus)
        )
        .errorReport(new ErrorReport())
    }

    // Setup api builders
    when {
      wsm.getControlledAzureResourceApi(any)(any)
    } thenReturn IO.pure(api)

    when {
      wsm.getResourceApi(any)(any)
    } thenReturn IO.pure(resourceApi)
    (wsm, api, resourceApi)
  }

}
