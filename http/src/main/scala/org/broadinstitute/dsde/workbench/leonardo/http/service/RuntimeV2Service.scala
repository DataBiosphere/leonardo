package org.broadinstitute.dsde.workbench.leonardo.http.service

import cats.mtl.Ask
import org.broadinstitute.dsde.workbench.model.UserInfo
import org.broadinstitute.dsde.workbench.leonardo.http.api.ListRuntimeResponse2
import org.broadinstitute.dsde.workbench.leonardo.{WorkspaceId, AppContext, RuntimeName}

trait RuntimeV2Service[F[_]] {
  def listRuntimes(userInfo: UserInfo, runtimeName: RuntimeName, workspaceId: Option[WorkspaceId])(
    implicit as: Ask[F, AppContext]
  ): F[Vector[ListRuntimeResponse2]]
}
