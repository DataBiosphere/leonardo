package org.broadinstitute.dsde.workbench.leonardo
package dao

import cats.effect.IO
import cats.mtl.Ask
import org.broadinstitute.dsde.workbench.azure._
import org.http4s.headers.Authorization

import java.util.UUID

class MockWsmDAO(jobStatus: WsmJobStatus = WsmJobStatus.Succeeded) extends WsmDao[IO] {
  override def getLandingZoneResources(billingProfileId: BillingProfileId, userToken: Authorization)(implicit
    ev: Ask[IO, AppContext]
  ): IO[LandingZoneResources] =
    IO.pure(
      CommonTestData.landingZoneResources
    )

  override def getWorkspaceStorageContainer(workspaceId: WorkspaceId, authorization: Authorization)(implicit
    ev: Ask[IO, AppContext]
  ): IO[Option[StorageContainerResponse]] =
    IO.pure(Some(StorageContainerResponse(ContainerName("dummy"), WsmControlledResourceId(UUID.randomUUID()))))
}
