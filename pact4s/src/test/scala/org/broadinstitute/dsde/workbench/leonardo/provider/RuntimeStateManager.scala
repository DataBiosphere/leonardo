package org.broadinstitute.dsde.workbench.leonardo.provider

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import cats.mtl.Ask
import org.broadinstitute.dsde.workbench.google2.DiskName
import org.broadinstitute.dsde.workbench.leonardo.CommonTestData._
import org.broadinstitute.dsde.workbench.leonardo._
import org.broadinstitute.dsde.workbench.leonardo.http.service._
import org.broadinstitute.dsde.workbench.leonardo.http.{CreateRuntimeRequest, CreateRuntimeResponse, DiskConfig, GetRuntimeResponse, UpdateRuntimeRequest}
import org.broadinstitute.dsde.workbench.leonardo.model.{RuntimeAlreadyExistsException, RuntimeNotFoundException}
import org.broadinstitute.dsde.workbench.model.google.{GcsBucketName, GcsObjectName, GcsPath, GoogleProject}
import org.broadinstitute.dsde.workbench.model.{TraceId, UserInfo}
import org.mockito.ArgumentMatchers.{any, anyString}
import org.mockito.Mockito.when
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
    Set(jupyterImage, welderImage, proxyImage, cryptoDetectorImage).map(_.copy(timestamp = date)),
    defaultScopes,
    welderEnabled = true,
    patchInProgress = true,
    Map("ev1" -> "a", "ev2" -> "b"),
    Some(DiskConfig(DiskName("disk"), DiskSize(100), DiskType.Standard, BlockSize(1024)))
  )

  private def mockCreateRuntime(mockRuntimeService: RuntimeService[IO], mockResponse: IO[CreateRuntimeResponse]): IO[Unit] = for {
    _ <- IO(
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
    )
  } yield ()

  private def mockGetRuntime(mockRuntimeService: RuntimeService[IO], mockResponse: IO[GetRuntimeResponse]): IO[Unit] = for {
    _ <- IO(
      when {
        mockRuntimeService.getRuntime(any[UserInfo], any[CloudContext.Gcp], RuntimeName(anyString()))(
          any[Ask[IO, AppContext]]
        )
      } thenReturn {
        mockResponse
      }
    )
  } yield ()

  private def mockUpdateRuntime(mockRuntimeService: RuntimeService[IO], mockResponse: IO[Unit]): IO[Unit] = for {
    _ <- IO(
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
    )
  } yield ()

  def handler(mockRuntimeService: RuntimeService[IO]): PartialFunction[ProviderState, Unit] = {
    case ProviderState(States.RuntimeExists, _) =>
      mockUpdateRuntime(mockRuntimeService, IO.unit).unsafeRunSync()
      when(
        mockRuntimeService.stopRuntime(
          any[UserInfo],
          any[CloudContext.Gcp],
          RuntimeName(anyString())
        )(
          any[Ask[IO, AppContext]]
        )
      ).thenReturn(IO.unit)
      when(
        mockRuntimeService.deleteRuntime(any[DeleteRuntimeRequest])(
          any[Ask[IO, AppContext]]
        )
      ).thenReturn(IO.unit)

      mockGetRuntime(mockRuntimeService, IO {
        mockedGetRuntimeResponse
      }).unsafeRunSync()
      mockCreateRuntime(mockRuntimeService, IO.raiseError(
        RuntimeAlreadyExistsException(CloudContext.Gcp(GoogleProject("123")),
          RuntimeName("nonexistentruntimename"),
          RuntimeStatus.Running
        )
      )).unsafeRunSync()
    case ProviderState(States.RuntimeDoesNotExist, _) =>
      mockCreateRuntime(mockRuntimeService, IO(CreateRuntimeResponse(TraceId("test")))).unsafeRunSync()
      mockGetRuntime(mockRuntimeService, IO.raiseError(
        RuntimeNotFoundException(CloudContext.Gcp(GoogleProject("123")),
          RuntimeName("nonexistentruntimename"),
          "OOOPS"
        )
      )).unsafeRunSync()
      mockUpdateRuntime(mockRuntimeService, IO.raiseError(
        RuntimeNotFoundException(CloudContext.Gcp(GoogleProject("123")),
          RuntimeName("nonexistentruntimename"),
          "Unable to find the runtime that you are trying to update"
        )
      )).unsafeRunSync()
      when(
        mockRuntimeService.deleteRuntime(any[DeleteRuntimeRequest])(
          any[Ask[IO, AppContext]]
        )
      ).thenReturn(
        IO.raiseError(
          RuntimeNotFoundException(CloudContext.Gcp(GoogleProject("123")),
                                   RuntimeName("nonexistentruntimename"),
                                   "Unable to find the runtime that you are trying to update"
          )
        )
      )
  }
}
