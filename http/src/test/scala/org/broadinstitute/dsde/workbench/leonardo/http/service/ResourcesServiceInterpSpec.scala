package org.broadinstitute.dsde.workbench.leonardo
package http
package service

import cats.effect.IO
import cats.mtl.Ask
import org.broadinstitute.dsde.workbench.leonardo.CommonTestData.{
  allowListAuthProvider,
  project,
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
  // User passes isUserProjectReader
  when(mockAllowListAuthProvider.isUserProjectReader(any, any)(any)).thenReturn(IO.pure(true))

  val mockRuntimeService = mock[RuntimeServiceInterp[IO]](defaultMockitoAnswer[IO])
  val mockAppService = mock[LeoAppServiceInterp[IO]](defaultMockitoAnswer[IO])
  val mockDiskService = mock[DiskServiceInterp[IO]](defaultMockitoAnswer[IO])
  val mockResourcesService =
    new ResourcesServiceInterp(mockAllowListAuthProvider, mockRuntimeService, mockAppService, mockDiskService)

  it should "fail deleteAll if user doesn't have project level permission" in {
    an[ForbiddenError] should be thrownBy {
      resourcesService
        .deleteAllResources(unauthorizedUserInfo, project, true, true)
        .unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
    }
  }

  it should "call deleteAllAppsRecords, deleteAllRuntimesRecords and deleteAllDisksRecords when deleteInCloud flag is false and deleteDisk is true" in {
    mockResourcesService.deleteAllResources(
      userInfo,
      project,
      false,
      true
    )
    verify(mockRuntimeService, times(1)).deleteAllRuntimesRecords(userInfo, CloudContext.Gcp(project))
    verify(mockAppService, times(1)).deleteAllAppsRecords(userInfo, CloudContext.Gcp(project))
    verify(mockDiskService, times(1)).deleteAllDisksRecords(userInfo, CloudContext.Gcp(project))
  }

  it should "call deleteAllApps, deleteAllRuntimes and deleteAllOrphanedDisks when deleteInCloud flag is true and deleteDisk is true" in {
    mockResourcesService.deleteAllResources(
      userInfo,
      project,
      true,
      true
    )
    verify(mockRuntimeService, times(1)).deleteAllRuntimes(userInfo, CloudContext.Gcp(project), true)
    verify(mockAppService, times(1)).deleteAllApps(userInfo, CloudContext.Gcp(project), true)
    verify(mockDiskService, times(1)).deleteAllOrphanedDisks(userInfo,
                                                             CloudContext.Gcp(project),
                                                             Some(diskIds),
                                                             diskNames
    )
  }

  it should "not call deleteAllOrphanedDisks when deleteInCloud flag is true and deleteDisk is false" in {
    mockResourcesService.deleteAllResources(
      userInfo,
      project,
      true,
      true
    )
    verify(mockRuntimeService, times(1)).deleteAllRuntimes(userInfo, CloudContext.Gcp(project), true)
    verify(mockAppService, times(1)).deleteAllApps(userInfo, CloudContext.Gcp(project), true)
    verify(mockDiskService, never()).deleteAllOrphanedDisks(userInfo,
                                                            CloudContext.Gcp(project),
                                                            Some(diskIds),
                                                            diskNames
    )
  }
}
