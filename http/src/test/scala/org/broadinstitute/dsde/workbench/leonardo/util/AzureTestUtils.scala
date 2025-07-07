package org.broadinstitute.dsde.workbench.leonardo.util

import bio.terra.workspace.api.{ControlledAzureResourceApi, ResourceApi, WorkspaceApi}
import bio.terra.workspace.model._
import cats.effect.IO
import cats.mtl.Ask
import com.azure.resourcemanager.compute.models.{PowerState, VirtualMachine}
import org.broadinstitute.dsde.workbench.azure.AzureCloudContext
import org.broadinstitute.dsde.workbench.azure.mock.FakeAzureVmService
import org.broadinstitute.dsde.workbench.leonardo.AppContext
import org.broadinstitute.dsde.workbench.leonardo.CommonTestData.wsmWorkspaceDesc
import org.broadinstitute.dsde.workbench.model.TraceId
import org.broadinstitute.dsde.workbench.model.google.GoogleProject
import org.broadinstitute.dsde.workbench.util2.InstanceName
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito.when
import org.scalatestplus.mockito.MockitoSugar
import reactor.core.publisher.Mono

import java.util.UUID
import scala.collection.mutable

object AzureTestUtils extends MockitoSugar {
  implicit val appContext: Ask[IO, AppContext] = AppContext
    .lift[IO](None, "")
    .unsafeRunSync()(cats.effect.unsafe.IORuntime.global)

  def setUpMockWsmApiClientProvider(
    diskJobStatus: JobReport.StatusEnum = JobReport.StatusEnum.SUCCEEDED,
    vmJobStatus: JobReport.StatusEnum = JobReport.StatusEnum.SUCCEEDED,
    storageContainerJobStatus: JobReport.StatusEnum = JobReport.StatusEnum.SUCCEEDED,
    googleProject: Option[GoogleProject] = None
  ): (ControlledAzureResourceApi, ResourceApi, WorkspaceApi) = {
    val api = mock[ControlledAzureResourceApi]
    val workspaceApi = mock[WorkspaceApi]
    val resourceApi = mock[ResourceApi]
    val disksByJob = mutable.Map.empty[String, CreateControlledAzureDiskRequestV2Body]

    // Create disk v2
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
        .errorReport(new ErrorReport().message("test exception"))
    }

    // Create disk
    when {
      api.createAzureDisk(any, any)
    } thenAnswer { _ =>
      new CreateControlledAzureResourceResult()
        .jobReport(
          new JobReport().status(diskJobStatus)
        )
        .errorReport(new ErrorReport().message("test exception"))
    }

    // Get disk result
    when {
      api.getCreateAzureDiskResult(any, any)
    } thenAnswer { _ =>
      new CreateControlledAzureResourceResult()
        .jobReport(
          new JobReport().status(diskJobStatus)
        )
        .errorReport(new ErrorReport().message("test exception"))
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

    // create vm result
    when {
      api.createAzureVm(any, any)
    } thenAnswer { _ =>
      new CreatedControlledAzureVmResult()
        .jobReport(
          new JobReport().status(vmJobStatus)
        )
        .azureVm(new AzureVmResource().attributes(new AzureVmAttributes().region("southcentralus")))
        .errorReport(new ErrorReport().message("test exception"))
    }

    // create vm result
    when {
      api.getCreateAzureVmResult(any, any)
    } thenAnswer { _ =>
      new CreatedControlledAzureVmResult()
        .jobReport(
          new JobReport().status(vmJobStatus)
        )
        .azureVm(new AzureVmResource().attributes(new AzureVmAttributes().region("southcentralus")))
        .errorReport(new ErrorReport().message("test exception"))
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

    (api, resourceApi, workspaceApi)
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
