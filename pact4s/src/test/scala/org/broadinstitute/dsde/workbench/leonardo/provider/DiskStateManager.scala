package org.broadinstitute.dsde.workbench.leonardo.provider

import cats.effect.IO
import cats.mtl.Ask
import org.broadinstitute.dsde.workbench.google2.{DiskName, ZoneName}
import org.broadinstitute.dsde.workbench.leonardo.http.{
  GetPersistentDiskResponse,
  ListPersistentDiskResponse,
  UpdateDiskRequest
}
import org.broadinstitute.dsde.workbench.leonardo.http.service.{DiskNotFoundException, DiskService}
import org.broadinstitute.dsde.workbench.leonardo.{
  AppContext,
  AuditInfo,
  BlockSize,
  CloudContext,
  DiskId,
  DiskSize,
  DiskStatus,
  DiskType,
  FormattedBy,
  SamResourceId,
  WorkspaceId
}
import org.broadinstitute.dsde.workbench.model.google.GoogleProject
import org.broadinstitute.dsde.workbench.model.{TraceId, UserInfo, WorkbenchEmail}
import org.mockito.ArgumentMatchers.{any, anyString}
import org.mockito.Mockito.when
import org.mockito.stubbing.OngoingStubbing
import pact4s.provider._

import java.time.Instant
import java.util.UUID

object DiskStateManager {
  object States {
    final val DiskExists = "there is a disk in a Google project"
    final val DiskDoesNotExist = "there is not a disk in a Google project"
    final val GoogleProjectWithDisksExists = "there is a Google project with disks"
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
    Some(WorkspaceId(UUID.randomUUID))
  )

  private val mockedListDiskResponse = ListPersistentDiskResponse(
    DiskId(1L),
    CloudContext.Gcp(GoogleProject("exampleProject")),
    ZoneName("exampleZone"),
    DiskName("exampleDiskName"),
    DiskStatus.Ready,
    AuditInfo(WorkbenchEmail(""), Instant.now(), None, Instant.now()),
    DiskSize(100),
    DiskType.SSD,
    BlockSize(1024),
    Map("labelA" -> "first"),
    Some(WorkspaceId(UUID.randomUUID))
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

  private def mockUpdateDisk(mockDiskService: DiskService[IO], mockResponse: IO[Unit]): OngoingStubbing[IO[Unit]] =
    when {
      mockDiskService.updateDisk(any[UserInfo], any[GoogleProject], DiskName(anyString()), any[UpdateDiskRequest])(
        any[Ask[IO, AppContext]]
      )
    } thenReturn {
      mockResponse
    }

  private def mockListDisksByProject(mockDiskService: DiskService[IO],
                                     mockResponse: IO[Vector[ListPersistentDiskResponse]]
  ): OngoingStubbing[IO[Vector[ListPersistentDiskResponse]]] =
    when {
      mockDiskService.listDisks(any[UserInfo], any[Option[CloudContext.Gcp]], any[Map[String, String]])(
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
      mockUpdateDisk(mockDiskService, IO.unit)
    case ProviderState(States.DiskDoesNotExist, _) =>
      mockGetDisk(
        mockDiskService,
        IO {
          throw DiskNotFoundException(CloudContext.Gcp(GoogleProject("exampleProject")),
                                      DiskName("exampleDiskName"),
                                      TraceId("exampleTraceId")
          )
        }
      )
      mockUpdateDisk(
        mockDiskService,
        IO {
          throw DiskNotFoundException(CloudContext.Gcp(GoogleProject("exampleProject")),
                                      DiskName("exampleDiskName"),
                                      TraceId("exampleTraceId")
          )
        }
      )
    case ProviderState(States.GoogleProjectWithDisksExists, _) =>
      mockListDisksByProject(mockDiskService,
                             IO {
                               Vector(mockedListDiskResponse)
                             }
      )
  }
}
