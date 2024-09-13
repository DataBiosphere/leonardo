package org.broadinstitute.dsde.workbench.leonardo.dao.sam

import cats.mtl.Ask
import org.broadinstitute.dsde.workbench.azure.AzureCloudContext
import org.broadinstitute.dsde.workbench.leonardo.{
  AppContext,
  CloudContext,
  SamPolicyData,
  SamResourceAction,
  SamResourceId,
  SamResourceType,
  WorkspaceId
}
import org.broadinstitute.dsde.workbench.model.WorkbenchEmail
import org.broadinstitute.dsde.workbench.model.google.GoogleProject

/**
 * This class contains Leonardo's interactions with Sam.
 */
trait SamService[F[_]] {

  /**
   * Gets a user's pet GCP service account, using the user's token.
   * @param bearerToken the user's access token
   * @param googleProject the google project of the pet
   * @param ev application context
   * @return email of the pet service account, or SamException if the pet
   *         could not be retrieved.
   */
  def getPetServiceAccount(bearerToken: String, googleProject: GoogleProject)(implicit
    ev: Ask[F, AppContext]
  ): F[WorkbenchEmail]

  /**
   * Gets a user's pet Azure managed identity, using the user's token.
   * @param bearerToken the user's access token
   * @param azureCloudContext the Azure cloud context
   * @param ev application context
   * @return email of the pet managed identity, or SamException if the pet
   *         could not be retrieved.
   */
  def getPetManagedIdentity(bearerToken: String, azureCloudContext: AzureCloudContext)(implicit
    ev: Ask[F, AppContext]
  ): F[WorkbenchEmail]

  /**
   * Gets a user's pet GCP service account or Azure managed identity using the user's token.
   * @param bearerToken the user's access token
   * @param cloudContext GCP or Azure cloud context.
   * @param ev application context
   * @return email of the pet service account or pet managed identity, or SamException if
   *         the pet could not be retrieved.
   */
  def getPetServiceAccountOrManagedIdentity(bearerToken: String, cloudContext: CloudContext)(implicit
    ev: Ask[F, AppContext]
  ): F[WorkbenchEmail] = cloudContext match {
    case CloudContext.Gcp(googleProject)       => getPetServiceAccount(bearerToken, googleProject)
    case CloudContext.Azure(azureCloudContext) => getPetManagedIdentity(bearerToken, azureCloudContext)
  }

  /**
   * Gets a user's proxy group, using a Leonardo token.
   * @param userEmail the user email
   * @param ev application context
   * @return email of the proxy group, or SamException if the pet could not be retrieved.
   */
  def getProxyGroup(
    userEmail: WorkbenchEmail
  )(implicit ev: Ask[F, AppContext]): F[WorkbenchEmail]

  /**
   * Gets a token for a user's pet service account, using a Leonardo token.
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
   * Gets a token for a user's arbitrary pet service account, using a Leonardo token.
   * @param userEmail the user email
   * @param ev application context
   * @return access token for the user's arbitrary pet service account, or SamException if
   *         the pet could not be retrieved.
   */
  def getArbitraryPetServiceAccountToken(userEmail: WorkbenchEmail)(implicit ev: Ask[F, AppContext]): F[String]

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
   * @param bearerToken the user's access token
   * @param googleProject the google project whose workspace parent to look up
   * @param ev application context
   * @return optional workspace ID
   */
  def lookupWorkspaceParentForGoogleProject(bearerToken: String, googleProject: GoogleProject)(implicit
    ev: Ask[F, AppContext]
  ): F[Option[WorkspaceId]]

  /**
   * Checks whether a user can perform an action on a Sam resource.
   * @param bearerToken the user's access token
   * @param samResourceId the Sam resource ID
   * @param action the Sam action
   * @param ev application context
   * @return Unit if authorized, ForbiddenError if not authorized, SamException on errors.
   */
  def checkAuthorized(bearerToken: String, samResourceId: SamResourceId, action: SamResourceAction)(implicit
    ev: Ask[F, AppContext]
  ): F[Unit]

  /**
   * List all Sam resources a user has access to.
   * @param bearerToken the user's access token
   * @param samResourceType Sam resource type to list
   * @param ev application context
   * @return list of Sam resource IDs, or SamException on errors.
   */
  def listResources(bearerToken: String, samResourceType: SamResourceType)(implicit
    ev: Ask[F, AppContext]
  ): F[List[String]]

  /**
   * Creates a Sam resource.
   * @param bearerToken the user's access token
   * @param samResourceId the Sam resource ID
   * @param parentProject optional parent google project resource.
   *                      One of parentProject or parentWorkspace is required.
   * @param parentWorkspace optional parent workspace resource.
   *                        One of parentProject or parentWorkspace is required.
   * @param policies optional mapping of policy name to policy data for the Sam resource.
   *                 Note policy name can be an arbitrary string, but must be unique per
   *                 resource. Policy data contains emails and Sam roles.
   * @param ev application context
   * @return Unit, or SamException on errors.
   */
  def createResource(bearerToken: String,
                     samResourceId: SamResourceId,
                     parentProject: Option[GoogleProject],
                     parentWorkspace: Option[WorkspaceId],
                     policies: Map[String, SamPolicyData]
  )(implicit
    ev: Ask[F, AppContext]
  ): F[Unit]

  /**
   * Deletes a Sam resource.
   * @param userEmail the user's access token
   * @param samResourceId the Sam resource ID
   * @param ev application context
   * @return Unit, or SamException on errors
   */
  def deleteResource(bearerToken: String, samResourceId: SamResourceId)(implicit
    ev: Ask[F, AppContext]
  ): F[Unit]

  /**
   * Fetch the email from Sam associated with the user access token.
   * @param bearerToken the user's access token
   * @param ev application context
   * @return user email, or SamException on errors
   */
  def getUserEmail(bearerToken: String)(implicit ev: Ask[F, AppContext]): F[WorkbenchEmail]
}
