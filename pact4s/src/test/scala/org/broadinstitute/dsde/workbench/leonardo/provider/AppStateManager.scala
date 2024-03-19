package org.broadinstitute.dsde.workbench.leonardo.provider

import cats.effect.IO
import cats.mtl.Ask
import org.broadinstitute.dsde.workbench.google2.{DiskName, KubernetesSerializableName, MachineTypeName, RegionName}
import org.broadinstitute.dsde.workbench.leonardo.http.service.{
  AppAlreadyExistsException,
  AppNotFoundException,
  AppService
}
import org.broadinstitute.dsde.workbench.leonardo.http.{CreateAppRequest, GetAppResponse, ListAppResponse}
import org.broadinstitute.dsde.workbench.leonardo.{
  AppAccessScope,
  AppContext,
  AppError,
  AppName,
  AppStatus,
  AppType,
  AuditInfo,
  CloudContext,
  KubernetesRuntimeConfig,
  NumNodes,
  WorkspaceId
}
import org.broadinstitute.dsde.workbench.model.google.GoogleProject
import org.broadinstitute.dsde.workbench.model.{TraceId, UserInfo, WorkbenchEmail}
import org.broadinstitute.dsp.ChartName
import org.mockito.ArgumentMatchers.{any, anyString}
import org.mockito.Mockito.when
import org.mockito.stubbing.OngoingStubbing
import pact4s.provider._

import java.net.URL
import java.time.Instant
import java.util.UUID
object AppStateManager {
  object States {
    final val AppExists = "there is an app in a Google project"
    final val AppDoesNotExist = "there is not an app in a Google project"
    final val GoogleProjectWithAppsExists = "there is a Google project with apps"
  }

  private val mockedGetAppResponse = GetAppResponse(
    None,
    AppName("exampleApp"),
    CloudContext.Gcp(GoogleProject("exampleProject")),
    RegionName("exampleRegion"),
    KubernetesRuntimeConfig(NumNodes(8), MachineTypeName("exampleMachine"), autoscalingEnabled = true),
    List.empty[AppError],
    AppStatus.Unspecified,
    Map.empty[KubernetesSerializableName.ServiceName, URL],
    Some(DiskName("exampleDiskName")),
    Map.empty[String, String],
    AuditInfo(WorkbenchEmail(""), Instant.now(), None, Instant.now()),
    AppType.CromwellRunnerApp,
    ChartName(""),
    None,
    Map.empty[String, String],
    autodeleteEnabled = true,
    autodeleteThreshold = Some(30)
  )
  private val mockedAppNotFoundException =
    AppNotFoundException(mockedGetAppResponse.cloudContext,
                         mockedGetAppResponse.appName,
                         TraceId("test"),
                         "App not found"
    )
  private val mockedListAppResponse = ListAppResponse(
    Some(WorkspaceId(UUID.randomUUID())),
    CloudContext.Gcp(GoogleProject("exampleProject")),
    RegionName("exampleRegion"),
    KubernetesRuntimeConfig(NumNodes(8), MachineTypeName("exampleMachine"), autoscalingEnabled = true),
    List.empty[AppError],
    AppStatus.Running,
    Map.empty[KubernetesSerializableName.ServiceName, URL],
    AppName("exampleApp"),
    AppType.Cromwell,
    ChartName("mockedChartName"),
    Some(DiskName("exampleDiskName")),
    AuditInfo(WorkbenchEmail(""), Instant.now(), None, Instant.now()),
    Some(AppAccessScope.UserPrivate),
    Map.empty[String, String],
    autodeleteEnabled = true,
    autodeleteThreshold = Some(30)
  )

  private def mockGetApp(mockAppService: AppService[IO],
                         mockResponse: IO[GetAppResponse]
  ): OngoingStubbing[IO[GetAppResponse]] =
    when {
      mockAppService.getApp(any[UserInfo], any[CloudContext.Gcp], AppName(anyString()))(
        any[Ask[IO, AppContext]]
      )
    } thenReturn {
      mockResponse
    }

  private def mockDeleteApp(mockAppService: AppService[IO], mockResponse: IO[Unit]): OngoingStubbing[IO[Unit]] =
    when {
      mockAppService.deleteApp(any[UserInfo], any[CloudContext.Gcp], AppName(anyString()), any[Boolean])(
        any[Ask[IO, AppContext]]
      )
    } thenReturn {
      mockResponse
    }

  private def mockListApp(mockAppService: AppService[IO],
                          mockResponse: IO[Vector[ListAppResponse]]
  ): OngoingStubbing[IO[Vector[ListAppResponse]]] =
    when {
      mockAppService.listApp(any[UserInfo], any[Option[CloudContext.Gcp]], any[Map[String, String]])(
        any[Ask[IO, AppContext]]
      )
    } thenReturn {
      mockResponse
    }

  private def mockCreateApp(mockAppService: AppService[IO], mockResponse: IO[Unit]): OngoingStubbing[IO[Unit]] =
    when {
      mockAppService.createApp(any[UserInfo],
                               any[CloudContext.Gcp],
                               AppName(anyString()),
                               any[CreateAppRequest],
                               any[Option[WorkspaceId]]
      )(
        any[Ask[IO, AppContext]]
      )
    } thenReturn {
      mockResponse
    }

  def handler(mockAppService: AppService[IO]): PartialFunction[ProviderState, Unit] = {
    case ProviderState(States.AppExists, _) =>
      mockGetApp(mockAppService,
                 IO {
                   mockedGetAppResponse
                 }
      )
      mockDeleteApp(mockAppService, IO.unit)
      mockCreateApp(
        mockAppService,
        IO.raiseError(
          AppAlreadyExistsException(CloudContext.Gcp(GoogleProject("exampleProject")),
                                    AppName("mocked app name"),
                                    AppStatus.Running,
                                    TraceId("test")
          )
        )
      )
    case ProviderState(States.AppDoesNotExist, _) =>
      mockGetApp(mockAppService, IO.raiseError(mockedAppNotFoundException))
      mockDeleteApp(mockAppService, IO.raiseError(mockedAppNotFoundException))
      mockCreateApp(mockAppService, IO.unit)
    case ProviderState(States.GoogleProjectWithAppsExists, _) =>
      mockListApp(mockAppService,
                  IO {
                    Vector(mockedListAppResponse)
                  }
      )
  }
}
