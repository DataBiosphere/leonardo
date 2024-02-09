package org.broadinstitute.dsde.workbench.leonardo
package http
package service

import cats.effect.IO
import cats.mtl.Ask
import org.broadinstitute.dsde.workbench.leonardo.CommonTestData.{allowListAuthProvider, project, unauthorizedUserInfo}
import org.broadinstitute.dsde.workbench.leonardo.db.TestComponent
import org.broadinstitute.dsde.workbench.leonardo.model.ForbiddenError
import org.scalatest.flatspec.AnyFlatSpec

class ResourcesServiceInterpSpec
    extends AnyFlatSpec
    with AppServiceInterpSpec
    with DiskServiceInterpSpec
    with RuntimeServiceInterpSpec
    with LeonardoTestSuite
    with TestComponent {

  implicit override val appContext: Ask[IO, AppContext] = AppContext
    .lift[IO](None, "")
    .unsafeRunSync()(cats.effect.unsafe.IORuntime.global)

  val appService = appServiceInterp
  val (diskService, _) = makeDiskService()
  val resourcesService = new ResourcesServiceInterp(allowListAuthProvider, runtimeService, appService, diskService)

  it should "fail deleteAll if user doesn't have project level permission" in {
    an[ForbiddenError] should be thrownBy {
      resourcesService
        .deleteAllResources(unauthorizedUserInfo, project, true, true)
        .unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
    }
  }

  //  it should "call deleteAllAppsRecords, deleteAllRuntimesRecords and deleteAllDisksRecords when deleteInCloud flag is false and deleteDisk is true" in {}
  //  it should "call deleteAllApps, deleteAllRuntimes and deleteAllOrphanedDisks when deleteInCloud flag is true and deleteDisk is true" in {}
  //  it should "not call deleteAllOrphanedDisks when deleteInCloud flag is true and deleteDisk is false" in {}
}
