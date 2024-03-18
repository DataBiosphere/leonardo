package org.broadinstitute.dsde.workbench.leonardo.provider

import cats.effect.IO
import cats.mtl.Ask
import org.broadinstitute.dsde.workbench.google2.{DiskName, ZoneName}
import org.broadinstitute.dsde.workbench.leonardo.http.GetPersistentDiskResponse
import org.broadinstitute.dsde.workbench.leonardo.http.service.DiskService
import org.broadinstitute.dsde.workbench.leonardo.{AppContext, AuditInfo, BlockSize, CloudContext, DiskId, DiskSize, DiskStatus, DiskType, FormattedBy, LabelMap, SamResourceId}
import org.broadinstitute.dsde.workbench.model.google.GoogleProject
import org.broadinstitute.dsde.workbench.model.{UserInfo, WorkbenchEmail}
import org.mockito.ArgumentMatchers.{any, anyString}
import org.mockito.Mockito.when
import org.mockito.stubbing.OngoingStubbing
import pact4s.provider._

import java.time.Instant

object DiskStateManager {
  object States {
    final val DiskExists = "there is a disk in a Google project"
    final val DiskDoesNotExist = "there is not a disk in a Google project"
  }

  private val mockedGetPersistentDiskResponse = GetPersistentDiskResponse(
    DiskId(1L),
    CloudContext.Gcp(GoogleProject("exampleProject")),
    ZoneName("exampleZone"),
    DiskName("exampleDiskName"),
    WorkbenchEmail("example@email.com"),
    SamResourceId.PersistentDiskSamResourceId("exampleSamResourceId"),
    DiskStatus.Ready,
    AuditInfo(WorkbenchEmail(""), Instant.now(), None, Instant.now()),
    DiskSize(100),
    DiskType.SSD,
    BlockSize(1024),
    Map("labelA" -> "first"),
    Some(FormattedBy.GCE),
  )

  private def mockGetDisk(mockDiskService: DiskService[IO],
                          mockResponse: IO[GetPersistentDiskResponse]
  ): OngoingStubbing[IO[GetPersistentDiskResponse]] =
    when {
      mockDiskService.getDisk(any[UserInfo], any[CloudContext.Gcp], DiskName(anyString()))(
        any[Ask[IO, AppContext]]
      )
    } thenReturn {
      mockResponse
    }

  def handler(mockDiskService: DiskService[IO]): PartialFunction[ProviderState, Unit] = {
    case ProviderState(States.DiskExists, _) =>
      mockGetDisk(mockDiskService,
                  IO {
                    mockedGetPersistentDiskResponse
                  }
      )
  }
}
