package org.broadinstitute.dsde.workbench.leonardo.provider

import cats.effect.IO
import cats.mtl.Ask
import org.broadinstitute.dsde.workbench.google2.DiskName
import org.broadinstitute.dsde.workbench.leonardo.CommonTestData._
import org.broadinstitute.dsde.workbench.leonardo.http.service._
import org.broadinstitute.dsde.workbench.leonardo.http.{DiskConfig, GetRuntimeResponse}
import org.broadinstitute.dsde.workbench.leonardo._
import org.broadinstitute.dsde.workbench.model.UserInfo
import org.broadinstitute.dsde.workbench.model.google.{GcsBucketName, GcsObjectName, GcsPath}
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito.when
import org.scalacheck.Gen.uuid
import pact4s.provider._

import java.net.URL
import java.time.Instant

object RuntimeStateManager {
  private object States {
    final val RuntimeExists = "there is a runtime in a Google project"
    final val RuntimeDoesNotExist = "there is not a runtime in a Google project"
  }
  def handler(mockRuntimeService: RuntimeService[IO]): PartialFunction[ProviderState, Unit] = {
    case ProviderState(States.RuntimeExists, _) =>
      val date = Instant.parse("2020-11-20T17:23:24.650Z")
      when(
        mockRuntimeService.getRuntime(any[UserInfo], any[CloudContext.Gcp], any[RuntimeName])(any[Ask[IO, AppContext]])
      )
        .thenReturn(IO {
          GetRuntimeResponse(
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
        })
    case ProviderState(States.RuntimeDoesNotExist, _) =>
      val date = Instant.parse("2020-11-20T17:23:24.650Z")
      when(
        mockRuntimeService.getRuntime(any[UserInfo], any[CloudContext.Gcp], any[RuntimeName])(any[Ask[IO, AppContext]])
      )
        .thenReturn(IO {
          GetRuntimeResponse(
            -2,
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
            Map("sampleLabel" -> "sampleValue"),
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
        })
  }
}
