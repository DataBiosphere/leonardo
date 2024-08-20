package org.broadinstitute.dsde.workbench.leonardo.util

import scala.collection.mutable
import cats.effect.IO
import org.mockito.Mockito.when
import org.mockito.ArgumentMatchers.any
import bio.terra.workspace.api.{ControlledAzureResourceApi, ResourceApi, WorkspaceApi}
import bio.terra.workspace.model.{
  CreateControlledAzureDiskRequestV2Body,
  CreateControlledAzureResourceResult,
  CreatedControlledAzureStorageContainer,
  DeleteControlledAzureResourceResult,
  ErrorReport,
  IamRole,
  JobReport
}
import cats.mtl.Ask
import com.azure.resourcemanager.compute.models.{PowerState, VirtualMachine}
import org.broadinstitute.dsde.workbench.azure.{AzureCloudContext, ManagedResourceGroupName, SubscriptionId, TenantId}
import org.broadinstitute.dsde.workbench.azure.mock.FakeAzureVmService
import org.broadinstitute.dsde.workbench.leonardo.CommonTestData.wsmWorkspaceDesc
import org.broadinstitute.dsde.workbench.leonardo.{AppContext, WorkspaceId}
import org.broadinstitute.dsde.workbench.leonardo.dao.{
  MockWsmClientProvider,
  WorkspaceDescription,
  WsmApiClientProvider
}
import org.broadinstitute.dsde.workbench.model.TraceId
import org.broadinstitute.dsde.workbench.model.google.GoogleProject
import org.broadinstitute.dsde.workbench.util2.InstanceName
import org.scalatestplus.mockito.MockitoSugar
import reactor.core.publisher.Mono

import java.util.UUID

object AzureTestUtils extends MockitoSugar {
  implicit val appContext: Ask[IO, AppContext] = AppContext
    .lift[IO](None, "")
    .unsafeRunSync()(cats.effect.unsafe.IORuntime.global)

  def setUpMockWsmApiClientProvider(
    diskJobStatus: JobReport.StatusEnum = JobReport.StatusEnum.SUCCEEDED,
    vmJobStatus: JobReport.StatusEnum = JobReport.StatusEnum.SUCCEEDED,
    storageContainerJobStatus: JobReport.StatusEnum = JobReport.StatusEnum.SUCCEEDED,
    googleProject: Option[GoogleProject] = None
  ): (WsmApiClientProvider[IO], ControlledAzureResourceApi, ResourceApi, WorkspaceApi) = {
    val api = mock[ControlledAzureResourceApi]
    val workspaceApi = mock[WorkspaceApi]
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
      api.createAzureStorageContainer(any, any)
    } thenAnswer { _ =>
      if (storageContainerJobStatus == JobReport.StatusEnum.SUCCEEDED)
        new CreatedControlledAzureStorageContainer().resourceId(UUID.randomUUID())
      else throw new Exception("storage container failed to create")
    }

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

    when {
      workspaceApi.getWorkspace(any(), any())
    } thenAnswer { invocation =>
      val workspaceId = invocation.getArgument[UUID](0)
      wsmWorkspaceDesc.id(workspaceId)
    }

    val wsm = new MockWsmClientProvider(api, resourceApi, workspaceApi) {
      override def getWorkspace(token: String, workspaceId: WorkspaceId, iamRole: IamRole)(implicit
        ev: Ask[IO, AppContext]
      ): IO[Option[WorkspaceDescription]] = {
        val azureContext =
          if (googleProject.isDefined) None
          else
            Some(
              AzureCloudContext(TenantId(workspaceId.value.toString),
                                SubscriptionId(workspaceId.value.toString),
                                ManagedResourceGroupName(workspaceId.value.toString)
              )
            )
        val googleContext = if (googleProject.isDefined) googleProject else None
        IO.pure(
          Some(
            WorkspaceDescription(
              workspaceId,
              "workspaceName" + workspaceId.value.toString,
              "spend-profile",
              azureContext,
              googleContext
            )
          )
        )
      }
    }

    (wsm, api, resourceApi, workspaceApi)
  }

  def setupFakeAzureVmService(startVm: Boolean = true,
                              stopVm: Boolean = true,
                              vmState: PowerState = PowerState.RUNNING
  ): FakeAzureVmService = {
    val vmReturn = mock[VirtualMachine]
    when(vmReturn.powerState()).thenReturn(vmState)

    new FakeAzureVmService {
      override def startAzureVm(name: InstanceName, cloudContext: AzureCloudContext)(implicit
        ev: Ask[IO, TraceId]
      ): IO[Option[Mono[Void]]] = if (startVm) IO.some(Mono.empty[Void]()) else IO.none

      override def stopAzureVm(name: InstanceName, cloudContext: AzureCloudContext)(implicit
        ev: Ask[IO, TraceId]
      ): IO[Option[Mono[Void]]] = if (stopVm) IO.some(Mono.empty[Void]()) else IO.none

      override def getAzureVm(name: InstanceName, cloudContext: AzureCloudContext)(implicit
        ev: Ask[IO, TraceId]
      ): IO[Option[VirtualMachine]] = IO.some(vmReturn)
    }
  }

}
