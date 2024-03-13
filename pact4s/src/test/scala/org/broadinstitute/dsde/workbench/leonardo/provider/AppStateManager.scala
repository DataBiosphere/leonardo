package org.broadinstitute.dsde.workbench.leonardo.provider

import cats.effect.IO
import cats.mtl.Ask
import org.broadinstitute.dsde.workbench.google2.{DiskName, KubernetesSerializableName, MachineTypeName, RegionName}
import org.broadinstitute.dsde.workbench.leonardo.http.GetAppResponse
import org.broadinstitute.dsde.workbench.leonardo.http.service.{AppNotFoundException, AppService}
import org.broadinstitute.dsde.workbench.leonardo.{
  AppContext,
  AppError,
  AppName,
  AppStatus,
  AppType,
  AuditInfo,
  CloudContext,
  KubernetesRuntimeConfig,
  NumNodes
}
import org.broadinstitute.dsde.workbench.model.google.GoogleProject
import org.broadinstitute.dsde.workbench.model.{UserInfo, WorkbenchEmail}
import org.broadinstitute.dsp.ChartName
import org.mockito.ArgumentMatchers.{any, anyString}
import org.mockito.Mockito.when
import org.mockito.stubbing.OngoingStubbing
import pact4s.provider._

import java.net.URL
import java.time.Instant
object AppStateManager {
  object States {
    final val AppExists = "there is an app in a Google project"
    final val AppDoesNotExist = "there is not an app in a Google project"
    final val GoogleProjectExists = "there is a Google project"
    final val GoogleProjectDoesNotExist = "there is not a Google project"
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
    AppNotFoundException(mockedGetAppResponse.cloudContext, mockedGetAppResponse.appName, null, "App not found")
  private val mockedGetAppResponseList = List(mockedGetAppResponse);

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
                          mockResponse: IO[List[GetAppResponse]]
  ): OngoingStubbing[IO[List[GetAppResponse]]] =
    when {
      mockAppService.listApp(any[UserInfo], any[Option[CloudContext.Gcp]], any[Map[String, String]])(
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
    case ProviderState(States.AppDoesNotExist, _) =>
      mockGetApp(mockAppService, IO.raiseError(mockedAppNotFoundException))
      mockDeleteApp(mockAppService, IO.raiseError(mockedAppNotFoundException))
    case ProviderState(States.GoogleProjectExists, _) =>
      mockListApp(mockAppService,
                  IO {
                    mockedGetAppResponseList
                  }
      )
  }
}
