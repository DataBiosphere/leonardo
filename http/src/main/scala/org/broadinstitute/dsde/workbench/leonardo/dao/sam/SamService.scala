package org.broadinstitute.dsde.workbench.leonardo.dao.sam

import cats.mtl.Ask
import org.broadinstitute.dsde.workbench.leonardo.{AppContext, WorkspaceId}
import org.broadinstitute.dsde.workbench.model.UserInfo
import org.broadinstitute.dsde.workbench.model.google.GoogleProject

/**
 * This class contains Leonardo's interactions with Sam.
 */
trait SamService[F[_]] {

  /**
   * Looks up the workspace parent Sam resource for the given google project.
   *
   * This method is used to populate workspaceId for Leonardo resources created
   * with the v1 routes, which are in terms of google project. Once Leonardo clients
   * are migrated to the v2 routes this method can be removed.
   *
   * Although we expect all google projects to have workspace parents in Sam, this
   * method will not fail if a workspace cannot be retrieved. Logs and metrics are
   * emitted for successful and failed workspace retrievals.
   *
   * @param userInfo the user info containing an access token
   * @param googleProject the google project whose workspace parent to look up
   * @param ev application context
   * @return optional workspace ID
   */
  def lookupWorkspaceParentForGoogleProject(userInfo: UserInfo, googleProject: GoogleProject)(implicit
    ev: Ask[F, AppContext]
  ): F[Option[WorkspaceId]]
}
