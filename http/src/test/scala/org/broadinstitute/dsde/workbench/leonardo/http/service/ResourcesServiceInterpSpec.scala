package org.broadinstitute.dsde.workbench.leonardo
package http
package service

import cats.effect.IO
import cats.mtl.Ask
import org.broadinstitute.dsde.workbench.leonardo.CommonTestData.{
  allowListAuthProvider,
  cloudContextGcp,
  unauthorizedUserInfo,
  userInfo
}
import org.broadinstitute.dsde.workbench.leonardo.TestUtils.defaultMockitoAnswer
import org.broadinstitute.dsde.workbench.leonardo.auth.AllowlistAuthProvider
import org.broadinstitute.dsde.workbench.leonardo.db.TestComponent
import org.broadinstitute.dsde.workbench.leonardo.http.service.MockAppService.diskNames
import org.broadinstitute.dsde.workbench.leonardo.http.service.MockRuntimeServiceInterp.diskIds
import org.broadinstitute.dsde.workbench.leonardo.model.ForbiddenError
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito.{never, times, verify, when}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatestplus.mockito.MockitoSugar

class ResourcesServiceInterpSpec
    extends AnyFlatSpec
    with AppServiceInterpSpec
    with DiskServiceInterpSpec
    with RuntimeServiceInterpSpec
    with LeonardoTestSuite
    with TestComponent
    with MockitoSugar {

  implicit override val appContext: Ask[IO, AppContext] = AppContext
    .lift[IO](None, "")
    .unsafeRunSync()(cats.effect.unsafe.IORuntime.global)

  val appService = gcpWorkspaceAppServiceInterp
  val (diskService, _) = makeDiskService()
  val resourcesService = new ResourcesServiceInterp(allowListAuthProvider, runtimeService, appService, diskService)

  val mockAllowListAuthProvider = mock[AllowlistAuthProvider](defaultMockitoAnswer[IO])
  when(mockAllowListAuthProvider.isUserProjectReader(any, any)(any)).thenReturn(IO.pure(true))

  val mockRuntimeService = mock[RuntimeServiceInterp[IO]](defaultMockitoAnswer[IO])
  when(mockRuntimeService.deleteAllRuntimesRecords(any, any)(any)).thenReturn(IO.unit)
  when(mockRuntimeService.deleteAllRuntimes(any, any, any)(any)).thenReturn(IO.pure(Some(diskIds)))

  val mockAppService = mock[LeoAppServiceInterp[IO]](defaultMockitoAnswer[IO])
  when(mockAppService.deleteAllAppsRecords(any, any)(any)).thenReturn(IO.unit)
  when(mockAppService.deleteAllApps(any, any, any)(any)).thenReturn(IO.pure(diskNames))

  val mockDiskService = mock[DiskServiceInterp[IO]](defaultMockitoAnswer[IO])
  when(mockDiskService.deleteAllDisksRecords(any, any)(any)).thenReturn(IO.unit)
  when(mockDiskService.deleteAllOrphanedDisks(any, any, any, any)(any)).thenReturn(IO.unit)

  val mockResourcesService =
    new ResourcesServiceInterp(mockAllowListAuthProvider, mockRuntimeService, mockAppService, mockDiskService)

  it should "fail deleteAll if user doesn't have project level permission" in {
    an[ForbiddenError] should be thrownBy {
      resourcesService
        .deleteAllResourcesInCloud(unauthorizedUserInfo, cloudContextGcp, true)
        .unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
    }
  }

  it should "call deleteAllAppsRecords should call deleteAllRuntimesRecords and deleteAllDisksRecords" in {
    mockResourcesService
      .deleteAllResourcesRecords(
        userInfo,
        cloudContextGcp
      )
      .unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
    verify(mockRuntimeService, times(1)).deleteAllRuntimesRecords(userInfo, cloudContextGcp)
    verify(mockAppService, times(1)).deleteAllAppsRecords(userInfo, cloudContextGcp)
    verify(mockDiskService, times(1)).deleteAllDisksRecords(userInfo, cloudContextGcp)
  }

  it should "not call deleteAllOrphanedDisks when deleteDisk is false" in {
    mockResourcesService
      .deleteAllResourcesInCloud(
        userInfo,
        cloudContextGcp,
        false
      )
      .unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
    verify(mockRuntimeService, times(1)).deleteAllRuntimes(userInfo, cloudContextGcp, false)
    verify(mockAppService, times(1)).deleteAllApps(userInfo, cloudContextGcp, false)
    verify(mockDiskService, never()).deleteAllOrphanedDisks(userInfo, cloudContextGcp, diskIds, diskNames)
  }

  it should "call deleteAllApps, deleteAllRuntimes and deleteAllOrphanedDisks when deleteDisk is true" in {
    mockResourcesService
      .deleteAllResourcesInCloud(
        userInfo,
        cloudContextGcp,
        true
      )
      .unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
    verify(mockRuntimeService, times(1)).deleteAllRuntimes(userInfo, cloudContextGcp, true)
    verify(mockAppService, times(1)).deleteAllApps(userInfo, cloudContextGcp, true)
    verify(mockDiskService, times(1)).deleteAllOrphanedDisks(userInfo, cloudContextGcp, diskIds, diskNames)
  }
}
