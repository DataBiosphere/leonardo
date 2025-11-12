package org.broadinstitute.dsde.workbench.leonardo
package http
package service

import cats.mtl.Ask
import org.broadinstitute.dsde.workbench.model.UserInfo

trait RuntimeV2Service[F[_]] {

  def listRuntimes(userInfo: UserInfo,
                   workspaceId: Option[WorkspaceId],
                   cloudProvider: Option[CloudProvider],
                   params: Map[String, String]
  )(implicit
    as: Ask[F, AppContext]
  ): F[Vector[ListRuntimeResponse2]]

  def startRuntime(userInfo: UserInfo, runtimeName: RuntimeName, workspaceId: WorkspaceId)(implicit
    as: Ask[F, AppContext]
  ): F[Unit]

  def stopRuntime(userInfo: UserInfo, runtimeName: RuntimeName, workspaceId: WorkspaceId)(implicit
    as: Ask[F, AppContext]
  ): F[Unit]
}
