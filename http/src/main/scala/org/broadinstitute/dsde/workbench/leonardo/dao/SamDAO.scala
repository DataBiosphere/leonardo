package org.broadinstitute.dsde.workbench.leonardo
package dao

import cats.mtl.Ask
import io.circe.Decoder
import org.broadinstitute.dsde.workbench.azure.AzureCloudContext
import org.broadinstitute.dsde.workbench.leonardo.SamResourceId.PrivateAzureStorageAccountSamResourceId
import org.broadinstitute.dsde.workbench.leonardo.model.{SamResource, SamResourceAction}
import org.broadinstitute.dsde.workbench.model.google.GoogleProject
import org.broadinstitute.dsde.workbench.model.{TraceId, UserInfo, WorkbenchEmail}
import org.broadinstitute.dsde.workbench.util.health.StatusCheckResponse
import org.http4s.headers.Authorization

/**
 * Deprecated. Functionality should be ported to SamService, which uses the generated Sam client.
 */
@Deprecated
trait SamDAO[F[_]] {

  /** Registers the Leo SA as a user in Sam. */
  def registerLeo(implicit ev: Ask[F, TraceId]): F[Unit]

  /** Calls Sam /status endpoint. */
  def getStatus(implicit ev: Ask[F, TraceId]): F[StatusCheckResponse]

  /** Checks whether the calling user has permission to do action A on a given resource R. */
  def hasResourcePermission[R, A](resource: R, action: A, authHeader: Authorization)(implicit
    sr: SamResourceAction[R, A],
    ev: Ask[F, TraceId]
  ): F[Boolean] =
    hasResourcePermissionUnchecked(sr.resourceType(resource),
                                   sr.resourceIdAsString(resource),
                                   sr.actionAsString(action),
                                   authHeader
    )

  /** Similar to `hasResourcePermission`, but makes it cacheable in Guava cache. */
  private[leonardo] def hasResourcePermissionUnchecked(resourceType: SamResourceType,
                                                       resource: String,
                                                       action: String,
                                                       authHeader: Authorization
  )(implicit
    ev: Ask[F, TraceId]
  ): F[Boolean]

  /** Returns all actions A the calling user has permission to perform on a given resource R. */
  def getListOfResourcePermissions[R, A](resource: R, authHeader: Authorization)(implicit
    sr: SamResourceAction[R, A],
    ev: Ask[F, TraceId]
  ): F[List[A]]

  /** For every role the user is granted on a Sam resource, list it and the resource ID. */
  def listResourceIdsWithRole[R <: SamResourceId](
    authHeader: Authorization
  )(implicit
    resourceDefinition: SamResource[R],
    resourceIdDecoder: Decoder[R],
    ev: Ask[F, TraceId]
  ): F[List[(R, SamRole)]]

  /** Returns all resources and actions (policies) the calling user has permission to for a given resource type.*/
  def getResourcePolicies[R](authHeader: Authorization, resourceType: SamResourceType)(implicit
    sr: SamResource[R],
    decoder: Decoder[R],
    ev: Ask[F, TraceId]
  ): F[List[(R, SamPolicyName)]]

  /** Returns all roles for the user for a given resource.*/
  def getResourceRoles(authHeader: Authorization, resourceId: SamResourceId)(implicit
    ev: Ask[F, TraceId]
  ): F[Set[SamRole]]

  /** Gets a pet GCP service account for the calling user. */
  def getPetServiceAccount(authorization: Authorization, googleProject: GoogleProject)(implicit
    ev: Ask[F, TraceId]
  ): F[Option[WorkbenchEmail]]

  /** Gets a pet Azure managed identity for the calling user. */
  def getPetManagedIdentity(authorization: Authorization, cloudContext: AzureCloudContext)(implicit
    ev: Ask[F, TraceId]
  ): F[Option[WorkbenchEmail]]

  /** Gets the GCP proxy group for provided user email. */
  def getUserProxy(userEmail: WorkbenchEmail)(implicit ev: Ask[F, TraceId]): F[Option[WorkbenchEmail]]

  /** Gets a GCP pet access token from cache for the provided user/project. */
  def getCachedPetAccessToken(userEmail: WorkbenchEmail, googleProject: GoogleProject)(implicit
    ev: Ask[F, TraceId]
  ): F[Option[String]]

  /** Gets a GCP pet access token from cache for the provided user in a shell project. */
  def getCachedArbitraryPetAccessToken(userEmail: WorkbenchEmail)(implicit
    ev: Ask[F, TraceId]
  ): F[Option[String]]

  /** Gets the subject ID for the provided user email using a GCP pet access token. */
  def getUserSubjectId(userEmail: WorkbenchEmail, googleProject: GoogleProject)(implicit
    ev: Ask[F, TraceId]
  ): F[Option[UserSubjectId]]

  /** Gets the Sam user info for the calling user. */
  def getSamUserInfo(token: String)(implicit ev: Ask[F, TraceId]): F[Option[SamUserInfo]]

  /** Gets the auth token for the Leo service account. */
  def getLeoAuthToken: F[Authorization]

  /** Returns whether the provided user is a member or admin of the given Sam group. */
  def isGroupMembersOrAdmin(groupName: GroupName, workbenchEmail: WorkbenchEmail)(implicit
    ev: Ask[F, TraceId]
  ): F[Boolean]

  /** Uses the user's token to query Sam's admin endpoint for their own user info.
    * Returns true of this query is successful, indicating that the user is an admin in Sam. */
  def isAdminUser(userInfo: UserInfo)(implicit
    ev: Ask[F, TraceId]
  ): F[Boolean]

  /** Gets an action managed identity from Sam as the calling user for the given resource type,
   * resource ID, and action. Returns the managed identity object ID. */
  def getAzureActionManagedIdentity(authHeader: Authorization,
                                    resource: PrivateAzureStorageAccountSamResourceId,
                                    action: PrivateAzureStorageAccountAction
  )(implicit ev: Ask[F, TraceId]): F[Option[String]]
}

final case class UserSubjectId(asString: String) extends AnyVal
final case class GroupName(asString: String) extends AnyVal
