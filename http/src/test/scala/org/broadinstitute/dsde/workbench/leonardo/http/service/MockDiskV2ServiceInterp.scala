package org.broadinstitute.dsde.workbench.leonardo
package http
package service

import cats.effect.IO
import cats.mtl.Ask
import org.broadinstitute.dsde.workbench.model.UserInfo

object MockDiskV2ServiceInterp extends DiskV2Service[IO] {

  def getDisk(userInfo: UserInfo, diskId: DiskId)(implicit
    as: Ask[IO, AppContext]
  ): IO[GetPersistentDiskV2Response] =
    IO.pure(
      GetPersistentDiskV2Response(
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
        CommonTestData.workspaceIdOpt,
        None
      )
    )

  def deleteDisk(userInfo: UserInfo, diskId: DiskId)(implicit
    as: Ask[IO, AppContext]
  ): IO[Unit] = IO.unit

}
