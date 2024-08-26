package org.broadinstitute.dsde.workbench.leonardo.dao.sam

import cats.mtl.Ask
import org.broadinstitute.dsde.workbench.azure.AzureCloudContext
import org.broadinstitute.dsde.workbench.leonardo.{AppContext, WorkspaceId}
import org.broadinstitute.dsde.workbench.model.google.GoogleProject
import org.broadinstitute.dsde.workbench.model.{UserInfo, WorkbenchEmail}

/**
 * This class contains Leonardo's interactions with Sam.
 */
trait SamService[F[_]] {

  /**
   * Gets a user's pet GCP service account, using the user's token.
   * @param userInfo the user info containing an access token
   * @param googleProject the google project of the pet
   * @param ev application context
   * @return email of the pet service account, or SamException if the pet
   *         could not be retrieved.
   */
  def getPetServiceAccount(userInfo: UserInfo, googleProject: GoogleProject)(implicit
    ev: Ask[F, AppContext]
  ): F[WorkbenchEmail]

  /**
   * Gets a user's pet Azure managed identity, using the user's token.
   * @param userInfo the user info containing an access token
   * @param azureCloudContext the Azure cloud context
   * @param ev application context
   * @return email of the pet managed identity, or SamException if the pet
   *         could not be retrieved.
   */
  def getPetManagedIdentity(userInfo: UserInfo, azureCloudContext: AzureCloudContext)(implicit
    ev: Ask[F, AppContext]
  ): F[WorkbenchEmail]

  /**
   * Gets the user's proxy group, using the Leo token.
   * @param userEmail the user email
   * @param ev application context
   * @return email of the proxy group, or SamException if the pet could not be retrieved.
   */
  def getProxyGroup(
    userEmail: WorkbenchEmail
  )(implicit ev: Ask[F, AppContext]): F[WorkbenchEmail]

  /**
   * Gets a token for the user's pet service account, using the Leo token.
   * @param userEmail the user email
   * @param googleProject the google project of the pet
   * @param ev application context
   * @return access token for the user's pet service account, or SamException if
   *         the pet could not be retrieved.
   */
  def getPetServiceAccountToken(userEmail: WorkbenchEmail, googleProject: GoogleProject)(implicit
    ev: Ask[F, AppContext]
  ): F[String]

  /**
   * Looks up the workspace parent Sam resource for the given google project.
   *
   * This method is not used for access control. It is used to populate the workspaceId
   * for Leonardo resources created with the v1 routes. Once Leonardo clients
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
