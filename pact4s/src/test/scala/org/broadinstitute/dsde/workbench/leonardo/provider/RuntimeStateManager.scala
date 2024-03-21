package org.broadinstitute.dsde.workbench.leonardo.provider

import cats.effect.IO
import cats.mtl.Ask
import org.broadinstitute.dsde.workbench.google2.DiskName
import org.broadinstitute.dsde.workbench.leonardo.CommonTestData._
import org.broadinstitute.dsde.workbench.leonardo._
import org.broadinstitute.dsde.workbench.leonardo.http.service._
import org.broadinstitute.dsde.workbench.leonardo.http.{
  CreateRuntimeRequest,
  CreateRuntimeResponse,
  DiskConfig,
  GetRuntimeResponse,
  UpdateRuntimeRequest
}
import org.broadinstitute.dsde.workbench.leonardo.model.{RuntimeAlreadyExistsException, RuntimeNotFoundException}
import org.broadinstitute.dsde.workbench.model.google.{GcsBucketName, GcsObjectName, GcsPath, GoogleProject}
import org.broadinstitute.dsde.workbench.model.{TraceId, UserInfo}
import org.mockito.ArgumentMatchers.{any, anyString}
import org.mockito.Mockito.when
import org.mockito.stubbing.OngoingStubbing
import org.scalacheck.Gen.uuid
import pact4s.provider._

import java.net.URL
import java.time.Instant

object RuntimeStateManager {
  object States {
    final val RuntimeExists = "there is a runtime in a Google project"
    final val RuntimeDoesNotExist = "there is not a runtime in a Google project"
  }

  private val date = Instant.parse("2020-11-20T17:23:24.650Z")
  private val mockedGetRuntimeResponse = GetRuntimeResponse(
    -1,
    runtimeSamResource,
    name1,
    cloudContextGcp,
    serviceAccountEmail,
    Some(makeAsyncRuntimeFields(1).copy(proxyHostName = ProxyHostName(uuid.toString))),
    auditInfo.copy(createdDate = date, dateAccessed = date),
    Some(date),
    defaultGceRuntimeConfig,
    new URL("https://leo.org/proxy"),
    RuntimeStatus.Running,
    Map("foo" -> "bar"),
    Some(UserScriptPath.Gcs(GcsPath(GcsBucketName("bucket-name"), GcsObjectName("userScript")))),
    Some(UserScriptPath.Gcs(GcsPath(GcsBucketName("bucket-name"), GcsObjectName("startScript")))),
    List.empty[RuntimeError],
    None,
    30,
    Some("clientId"),
    Set(jupyterImage, welderImage, proxyImage, sfkitImage, cryptoDetectorImage).map(_.copy(timestamp = date)),
    defaultScopes,
    welderEnabled = true,
    patchInProgress = true,
    Map("ev1" -> "a", "ev2" -> "b"),
    Some(DiskConfig(DiskName("disk"), DiskSize(100), DiskType.Standard, BlockSize(1024)))
  )
  private val mockedRuntimeNotFoundException = RuntimeNotFoundException(
    CloudContext.Gcp(GoogleProject("123")),
    RuntimeName("nonexistentruntimename"),
    "Unable to find the runtime that you are trying to update"
  )

  private def mockCreateRuntime(mockRuntimeService: RuntimeService[IO],
                                mockResponse: IO[CreateRuntimeResponse]
  ): OngoingStubbing[IO[CreateRuntimeResponse]] =
    when {
      mockRuntimeService.createRuntime(any[UserInfo],
                                       any[CloudContext.Gcp],
                                       RuntimeName(anyString()),
                                       any[CreateRuntimeRequest]
      )(
        any[Ask[IO, AppContext]]
      )
    } thenReturn {
      mockResponse
    }

  private def mockGetRuntime(mockRuntimeService: RuntimeService[IO],
                             mockResponse: IO[GetRuntimeResponse]
  ): OngoingStubbing[IO[GetRuntimeResponse]] =
    when {
      mockRuntimeService.getRuntime(any[UserInfo], any[CloudContext.Gcp], RuntimeName(anyString()))(
        any[Ask[IO, AppContext]]
      )
    } thenReturn {
      mockResponse
    }

  private def mockUpdateRuntime(mockRuntimeService: RuntimeService[IO],
                                mockResponse: IO[Unit]
  ): OngoingStubbing[IO[Unit]] =
    when(
      mockRuntimeService.updateRuntime(
        any[UserInfo],
        any[GoogleProject],
        RuntimeName(anyString()),
        any[UpdateRuntimeRequest]
      )(
        any[Ask[IO, AppContext]]
      )
    ).thenReturn(
      mockResponse
    )

  private def mockDeleteRuntime(mockRuntimeService: RuntimeService[IO],
                                mockResponse: IO[Unit]
  ): OngoingStubbing[IO[Unit]] =
    when(
      mockRuntimeService.deleteRuntime(any[DeleteRuntimeRequest])(
        any[Ask[IO, AppContext]]
      )
    ).thenReturn(
      mockResponse
    )

  private def mockStopRuntime(mockRuntimeService: RuntimeService[IO],
                              mockResponse: IO[Unit]
  ): OngoingStubbing[IO[Unit]] =
    when(
      mockRuntimeService.stopRuntime(
        any[UserInfo],
        any[CloudContext.Gcp],
        RuntimeName(anyString())
      )(
        any[Ask[IO, AppContext]]
      )
    ).thenReturn(
      mockResponse
    )

  def handler(mockRuntimeService: RuntimeService[IO]): PartialFunction[ProviderState, Unit] = {
    case ProviderState(States.RuntimeExists, _) =>
      mockCreateRuntime(
        mockRuntimeService,
        IO.raiseError(
          RuntimeAlreadyExistsException(CloudContext.Gcp(GoogleProject("123")),
                                        RuntimeName("nonexistentruntimename"),
                                        RuntimeStatus.Running
          )
        )
      )
      mockGetRuntime(mockRuntimeService,
                     IO {
                       mockedGetRuntimeResponse
                     }
      )
      mockUpdateRuntime(mockRuntimeService, IO.unit)
      mockDeleteRuntime(mockRuntimeService, IO.unit)
      mockStopRuntime(mockRuntimeService, IO.unit)

    case ProviderState(States.RuntimeDoesNotExist, _) =>
      mockCreateRuntime(mockRuntimeService, IO(CreateRuntimeResponse(TraceId("test"))))
      mockGetRuntime(mockRuntimeService,
                     IO.raiseError(
                       mockedRuntimeNotFoundException
                     )
      )
      mockUpdateRuntime(mockRuntimeService,
                        IO.raiseError(
                          mockedRuntimeNotFoundException
                        )
      )
      mockDeleteRuntime(mockRuntimeService,
                        IO.raiseError(
                          mockedRuntimeNotFoundException
                        )
      )
      mockStopRuntime(mockRuntimeService,
                      IO.raiseError(
                        mockedRuntimeNotFoundException
                      )
      )
  }
}
