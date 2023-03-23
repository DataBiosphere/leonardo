package org.broadinstitute.dsde.workbench.leonardo
package http
package service

import cats.effect.IO
import cats.mtl.Ask
import org.broadinstitute.dsde.workbench.model.UserInfo

object MockDiskV2ServiceInterp extends DiskV2Service[IO] {

  def getDisk(userInfo: UserInfo, workspaceId: WorkspaceId, diskId: DiskId)(implicit
    as: Ask[IO, AppContext]
  ): IO[GetPersistentDiskResponse] =
    IO.pure(
      GetPersistentDiskResponse(
        DiskId(-1),
        CommonTestData.cloudContextAzure,
        CommonTestData.zone,
        CommonTestData.diskName,
        CommonTestData.serviceAccount,
        CommonTestData.diskSamResource,
        DiskStatus.Ready,
        CommonTestData.auditInfo,
        CommonTestData.diskSize,
        CommonTestData.diskType,
        CommonTestData.blockSize,
        Map.empty,
        None
      )
    )

  def deleteDisk(userInfo: UserInfo, workspaceId: WorkspaceId, diskId: DiskId)(implicit
    as: Ask[IO, AppContext]
  ): IO[Unit] = IO.unit

}
