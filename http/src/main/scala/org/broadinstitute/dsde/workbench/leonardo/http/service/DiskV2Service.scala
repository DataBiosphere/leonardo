package org.broadinstitute.dsde.workbench.leonardo
package http
package service

import cats.mtl.Ask
import org.broadinstitute.dsde.workbench.model.UserInfo

trait DiskV2Service[F[_]] {

  // TODO: Implement rest of v2 routes: createDisk, listDisks, updateDisk
  def getDisk(userInfo: UserInfo, diskId: DiskId)(implicit
    as: Ask[F, AppContext]
  ): F[GetPersistentDiskV2Response]

  def deleteDisk(userInfo: UserInfo, diskId: DiskId)(implicit
    as: Ask[F, AppContext]
  ): F[Unit]

}
