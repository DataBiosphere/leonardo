package org.broadinstitute.dsde.workbench.leonardo
package auth

import akka.http.scaladsl.model.StatusCodes
import cats.data.NonEmptyList
import cats.effect.{Async, Sync}
import cats.mtl.Ask
import cats.syntax.all._
import io.circe.{Decoder, Encoder}
import org.broadinstitute.dsde.workbench.leonardo.JsonCodec._
import org.broadinstitute.dsde.workbench.leonardo.SamResourceId._
import org.broadinstitute.dsde.workbench.leonardo.dao.{AuthProviderException, GroupName, SamDAO}
import org.broadinstitute.dsde.workbench.leonardo.model._
import org.broadinstitute.dsde.workbench.model.google.GoogleProject
import org.broadinstitute.dsde.workbench.model.{TraceId, UserInfo, WorkbenchEmail}
import org.broadinstitute.dsde.workbench.openTelemetry.OpenTelemetryMetrics
import org.http4s.client.dsl.Http4sClientDsl
import org.http4s.headers.Authorization
import org.http4s.{AuthScheme, Credentials}
import org.typelevel.log4cats.StructuredLogger
import scalacache.Cache

import scala.concurrent.duration._

class SamAuthProvider[F[_]: OpenTelemetryMetrics](
  samDao: SamDAO[F],
  config: SamAuthProviderConfig,
  saProvider: ServiceAccountProvider[F],
  authCache: Cache[F, AuthCacheKey, Boolean]
)(implicit F: Async[F], logger: StructuredLogger[F])
    extends LeoAuthProvider[F]
    with Http4sClientDsl[F] {
  override def serviceAccountProvider: ServiceAccountProvider[F] = saProvider
  override def hasPermission[R, A](samResource: R, action: A, userInfo: UserInfo)(implicit
    sr: SamResourceAction[R, A],
    ev: Ask[F, TraceId]
  ): F[Boolean] = {
    val authHeader = Authorization(Credentials.Token(AuthScheme.Bearer, userInfo.accessToken.token))
    if (config.authCacheEnabled && sr.cacheableActions.contains(action)) {
      authCache.cachingF(
        AuthCacheKey(sr.resourceType(samResource),
                     sr.resourceIdAsString(samResource),
                     authHeader,
                     sr.actionAsString(action)
        )
      )(None) {
        checkPermission(sr.resourceType(samResource),
                        sr.resourceIdAsString(samResource),
                        sr.actionAsString(action),
                        authHeader
        )
      }
    } else {
      checkPermission(sr.resourceType(samResource),
                      sr.resourceIdAsString(samResource),
                      sr.actionAsString(action),
                      authHeader
      )
    }
  }

  private def checkPermission(samResourceType: SamResourceType,
                              samResource: String,
                              action: String,
                              authHeader: Authorization
  )(implicit
    ev: Ask[F, TraceId]
  ): F[Boolean] =
    for {
      traceId <- ev.ask
      res <- samDao.hasResourcePermissionUnchecked(samResourceType, samResource, action, authHeader).recoverWith {
        case e =>
          logger
            .info(Map("traceId" -> traceId.asString), e)(s"$action is not allowed for resource $samResource")
            .as(false)
      }
    } yield res

  override def hasPermissionWithProjectFallback[R, A](
    samResource: R,
    action: A,
    projectAction: ProjectAction,
    userInfo: UserInfo,
    googleProject: GoogleProject
  )(implicit sr: SamResourceAction[R, A], ev: Ask[F, TraceId]): F[Boolean] =
    for {
      // First check permission at the resource level
      resourcePermission <- hasPermission(samResource, action, userInfo)
      // Fall back to the project-level check if necessary
      res <- resourcePermission match {
        case true => Sync[F].pure(true)
        case _    => hasPermission(ProjectSamResourceId(googleProject), projectAction, userInfo)
      }
    } yield res

  override def getActions[R, A](
    samResource: R,
    userInfo: UserInfo
  )(implicit sr: SamResourceAction[R, A], ev: Ask[F, TraceId]): F[List[A]] = {
    val authorization = Authorization(Credentials.Token(AuthScheme.Bearer, userInfo.accessToken.token))
    for {
      listOfPermissions <- samDao
        .getListOfResourcePermissions[R, A](samResource, authorization)
      setOfPermissions = listOfPermissions.toSet
      callerActions = sr.allActions.collect { case a if setOfPermissions.contains(a) => a }
    } yield callerActions
  }

  def getActionsWithProjectFallback[R, A](samResource: R, googleProject: GoogleProject, userInfo: UserInfo)(implicit
    sr: SamResourceAction[R, A],
    ev: Ask[F, TraceId]
  ): F[(List[A], List[ProjectAction])] = {
    val authorization = Authorization(Credentials.Token(AuthScheme.Bearer, userInfo.accessToken.token))
    for {
      listOfPermissions <- samDao
        .getListOfResourcePermissions(samResource, authorization)
      setOfPermissions = listOfPermissions.toSet
      callerActions = sr.allActions.collect { case a if setOfPermissions.contains(a) => a }

      listOfProjectPermissions <- samDao.getListOfResourcePermissions(ProjectSamResourceId(googleProject),
                                                                      authorization
      )
      setOfProjectPermissions = listOfProjectPermissions.toSet
      projectCallerActions = ProjectAction.allActions.toList.collect {
        case a if setOfProjectPermissions.contains(a) => a
      }
    } yield (callerActions, projectCallerActions)
  }

  /**
   * Get all Sam resource IDs of the given type on which the user is granted supported permissions. Returns explicitly
   * granted policies only; users with workspace owner access, for example, can see all runtimes in the workspace, but
   * getAuthorizedIds[SamRuntimeResourceId] is not guaranteed to return all runtimes in that workspace. Works for any
   * Sam resource type except SharedApp and similar alternative app types; these require a specific Sam resource object
   * to determine permissions, as these are stored on the individual records.
   * @param isOwner whether to return only resources that the user is granted ownership on (has `ownerRoleName`,
   * and `ownerRoleName` is an element in `policyNames` on the SamResource)
   * @param userInfo the current user
   * @param samResource the resource archetype (App, Runtime, etc)
   * @param decoder converts Json -> SamResourceId, needed by SamDao
   * @param ev the request context
   * @tparam R SamResourceId
   * @return List[SamResourceId] of all resources of type R which are visible to the user at the given access level
   */
  override def getAuthorizedIds[R <: SamResourceId](
    isOwner: Boolean,
    userInfo: UserInfo
  )(implicit
    samResource: SamResource[R],
    decoder: Decoder[R],
    ev: Ask[F, TraceId]
  ): F[List[R]] = {
    val authHeader = Authorization(Credentials.Token(AuthScheme.Bearer, userInfo.accessToken.token))
    val ownerRole: SamRole = samResource.ownerRoleName
    val canOwnerRead: Boolean = samResource.policyNames.isEmpty || samResource.policyNames.exists {
      policyName: SamPolicyName =>
        ownerRole.asString == policyName.toString
    }
    for {
      resourcesAndPolicies: List[(R, SamPolicyName)] <- samDao
        .getResourcePolicies[R](authHeader, samResource.resourceType)

      authorizedPolicies =
        if (isOwner && !canOwnerRead) {
          logger.warn(
            s"Resource type ${samResource.resourceType} may be incorrectly configured: ownerRoleName not found in policyNames"
          )
          List.empty
        } else if (isOwner && canOwnerRead) {
          // Show only resources the user owns
          resourcesAndPolicies filter { case (_, policy) =>
            ownerRole.asString == policy.toString
          }
        } else if (!samResource.policyNames.isEmpty) {
          // Show only resources on which the user is granted a readable policy
          resourcesAndPolicies filter { case (_, policy) =>
            samResource.policyNames.contains(policy)
          }
        } else {
          // Show all resources the user is granted any role on (reader)
          resourcesAndPolicies
        }
      _ = println(s"111111111 get authorized IDs ${samResource.resourceType} filtered to ${authorizedPolicies}")
      authorizedIds: List[R] = authorizedPolicies.map { case (samResourceId, _) => samResourceId }
    } yield authorizedIds
  }

  override def filterUserVisible[R](resources: NonEmptyList[R], userInfo: UserInfo)(implicit
    sr: SamResource[R],
    decoder: Decoder[R],
    ev: Ask[F, TraceId]
  ): F[List[R]] = {
    val authHeader = Authorization(Credentials.Token(AuthScheme.Bearer, userInfo.accessToken.token))
    val resourceTypes = resources.map(r => sr.resourceType(r)).toList.toSet
    for {
      resourcePolicies <- resourceTypes.toList.flatTraverse(resourceType =>
        samDao.getResourcePolicies[R](authHeader, resourceType)
      )
      res = resourcePolicies.filter { case (samResourceId, policyName) =>
        sr.policyNames(samResourceId).contains(policyName)
      }
    } yield resources.filter(samResourceId => res.exists(_._1 == samResourceId))
  }

  def filterUserVisibleWithProjectFallback[R](
    resources: NonEmptyList[(GoogleProject, R)],
    userInfo: UserInfo
  )(implicit
    sr: SamResource[R],
    decoder: Decoder[R],
    ev: Ask[F, TraceId]
  ): F[List[(GoogleProject, R)]] = {
    val authHeader = Authorization(Credentials.Token(AuthScheme.Bearer, userInfo.accessToken.token))
    val resourceTypes = resources.map(r => sr.resourceType(r._2)).toList.toSet
    for {
      projectPolicies <- samDao.getResourcePolicies[ProjectSamResourceId](authHeader, SamResourceType.Project)
      owningProjects = projectPolicies.collect { case (r, SamPolicyName.Owner) =>
        r.googleProject
      }
      resourcePolicies <- resourceTypes.toList.flatTraverse(resourceType =>
        samDao.getResourcePolicies[R](authHeader, resourceType)
      )
      res = resourcePolicies.filter { case (r, pn) => sr.policyNames(r).contains(pn) }
    } yield resources.filter { case (project, r) =>
      owningProjects.contains(project) || res.exists(_._1 == r)
    }
  }

  def filterResourceProjectVisible[R](
    resources: NonEmptyList[(GoogleProject, R)],
    userInfo: UserInfo
  )(implicit
    sr: SamResource[R],
    decoder: Decoder[R],
    ev: Ask[F, TraceId]
  ): F[List[(GoogleProject, R)]] = {
    val authHeader = Authorization(Credentials.Token(AuthScheme.Bearer, userInfo.accessToken.token))
    val resourceTypes = resources.map(r => sr.resourceType(r._2)).toList.toSet

    for {
      projectPolicies <- samDao
        .getResourcePolicies[ProjectSamResourceId](authHeader, SamResourceType.Project)
      readingProjects = projectPolicies.map(_._1.googleProject).toSet
      ownedProjects = projectPolicies.collect { case (r, SamPolicyName.Owner) =>
        r.googleProject
      }
      resourcePolicies <- resourceTypes.toList.flatTraverse(resourceType =>
        samDao.getResourcePolicies[R](authHeader, resourceType)
      )
      res = resourcePolicies.filter { case (samResourceId, policyName) =>
        sr.policyNames(samResourceId).contains(policyName)
      }
    } yield resources.filter { case (project, samResourceId) =>
      // user must be a project owner or at least a reader on the project and resource
      ownedProjects.contains(project) || (readingProjects.contains(project) && res.exists(_._1 == samResourceId))
    }
  }

  override def isUserProjectReader(cloudContext: CloudContext, userInfo: UserInfo)(implicit
    ev: Ask[F, TraceId]
  ): F[Boolean] = {
    val samProjectResource = ProjectSamResourceId(GoogleProject(cloudContext.asString))
    val authHeader = Authorization(Credentials.Token(AuthScheme.Bearer, userInfo.accessToken.token))
    for {
      roles <- samDao.getResourceRoles(authHeader, samProjectResource)
    } yield roles.nonEmpty
  }

  def filterWorkspaceOwner(
    resources: NonEmptyList[WorkspaceResourceSamResourceId],
    userInfo: UserInfo
  )(implicit
    ev: Ask[F, TraceId]
  ): F[Set[WorkspaceResourceSamResourceId]] = {
    val authHeader = Authorization(Credentials.Token(AuthScheme.Bearer, userInfo.accessToken.token))
    for {
      workspacePolicies <- samDao
        .getResourcePolicies[WorkspaceResourceSamResourceId](authHeader, SamResourceType.Workspace)
      owningWorkspaces = workspacePolicies.collect { case (r, SamPolicyName.Owner) =>
        r
      }
    } yield owningWorkspaces.toSet
  }

  def isUserWorkspaceOwner(
    workspaceResource: WorkspaceResourceSamResourceId,
    userInfo: UserInfo
  )(implicit ev: Ask[F, TraceId]): F[Boolean] = {
    val authHeader = Authorization(Credentials.Token(AuthScheme.Bearer, userInfo.accessToken.token))
    for {
      roles <- samDao.getResourceRoles(authHeader, workspaceResource)
    } yield roles.contains(SamRole.Owner)
  }

  def filterWorkspaceReader(
    resources: NonEmptyList[WorkspaceResourceSamResourceId],
    userInfo: UserInfo
  )(implicit
    ev: Ask[F, TraceId]
  ): F[Set[WorkspaceResourceSamResourceId]] = {
    val authHeader = Authorization(Credentials.Token(AuthScheme.Bearer, userInfo.accessToken.token))
    for {
      workspacePolicies <- samDao
        .getResourcePolicies[WorkspaceResourceSamResourceId](authHeader, SamResourceType.Workspace)
      readingWorkspaces = workspacePolicies.map(_._1)
    } yield readingWorkspaces.toSet
  }

  def isUserWorkspaceReader(
    workspaceResource: WorkspaceResourceSamResourceId,
    userInfo: UserInfo
  )(implicit ev: Ask[F, TraceId]): F[Boolean] = {
    val authHeader = Authorization(Credentials.Token(AuthScheme.Bearer, userInfo.accessToken.token))
    for {
      roles <- samDao.getResourceRoles(authHeader, workspaceResource)
    } yield roles.nonEmpty
  }

  override def notifyResourceCreated[R](
    samResource: R,
    creatorEmail: WorkbenchEmail,
    googleProject: GoogleProject
  )(implicit sr: SamResource[R], encoder: Encoder[R], ev: Ask[F, TraceId]): F[Unit] =
    // Note: apps on GCP are defined with a google-project as a parent Sam resource.
    // Otherwise the Sam resource has no parent.
    if (sr.resourceType(samResource) != SamResourceType.App)
      samDao.createResourceAsGcpPet(samResource, creatorEmail, googleProject)
    else
      samDao.createResourceWithGoogleProjectParent(samResource, creatorEmail, googleProject)

  override def notifyResourceCreatedV2[R](
    samResource: R,
    creatorEmail: WorkbenchEmail,
    cloudContext: CloudContext,
    workspaceId: WorkspaceId,
    userInfo: UserInfo
  )(implicit sr: SamResource[R], encoder: Encoder[R], ev: Ask[F, TraceId]): F[Unit] =
    // Note: V2 apps on both GCP and azure are defined with a workspace as a parent Sam resource.
    // Otherwise the Sam resource has no parent.
    if (List(SamResourceType.App, SamResourceType.SharedApp).contains(sr.resourceType(samResource)))
      samDao.createResourceWithWorkspaceParent(samResource, creatorEmail, userInfo, workspaceId)
    else
      samDao.createResourceWithUserInfo(samResource, userInfo)

  override def notifyResourceDeleted[R](
    samResource: R,
    creatorEmail: WorkbenchEmail,
    googleProject: GoogleProject
  )(implicit sr: SamResource[R], ev: Ask[F, TraceId]): F[Unit] =
    samDao.deleteResourceAsGcpPet(samResource, creatorEmail, googleProject)

  override def notifyResourceDeletedV2[R](
    samResource: R,
    userInfo: UserInfo
  )(implicit sr: SamResource[R], ev: Ask[F, TraceId]): F[Unit] =
    samDao.deleteResourceWithUserInfo(samResource, userInfo)

  override def lookupOriginatingUserEmail[R](petOrUserInfo: UserInfo)(implicit ev: Ask[F, TraceId]): F[WorkbenchEmail] =
    for {
      traceId <- ev.ask
      samUserInfoOpt <- samDao.getSamUserInfo(petOrUserInfo.accessToken.token)
      samUserInfo <- F.fromOption(
        samUserInfoOpt,
        AuthProviderException(
          traceId,
          s"[SamAuthProvider.lookupOriginatingUserEmail] Subject info not found for ${petOrUserInfo.userEmail.value}",
          StatusCodes.Unauthorized
        )
      )
      _ <-
        if (samUserInfo.enabled) F.unit
        else
          F.raiseError(
            AuthProviderException(
              traceId,
              s"[SamAuthProvider.lookupOriginatingUserEmail] User ${samUserInfo.userEmail.value} is disabled",
              StatusCodes.Forbidden
            )
          )
    } yield samUserInfo.userEmail

  /**
   * Confirm the Sam user is enabled (accepted terms of service etc).
   * Raises an AuthProviderException if user does not exist or is disabled.
   */
  override def checkUserEnabled(petOrUserInfo: UserInfo)(implicit ev: Ask[F, TraceId]): F[Unit] =
    for {
      traceId <- ev.ask
      samUserInfoOpt <- samDao.getSamUserInfo(petOrUserInfo.accessToken.token)
      samUserInfo <- F.fromOption(
        samUserInfoOpt,
        AuthProviderException(
          traceId,
          s"[SamAuthProvider.checkUserEnabled] Subject info not found for ${petOrUserInfo.userEmail.value}",
          StatusCodes.Unauthorized
        )
      )
      _ <-
        if (samUserInfo.enabled) F.unit
        else
          F.raiseError(
            AuthProviderException(
              traceId,
              s"[SamAuthProvider.checkUserEnabled] User ${samUserInfo.userEmail.value} is disabled",
              StatusCodes.Forbidden
            )
          )
    } yield ()

  override def isCustomAppAllowed(userEmail: WorkbenchEmail)(implicit ev: Ask[F, TraceId]): F[Boolean] =
    samDao.isGroupMembersOrAdmin(config.customAppCreationAllowedGroup, userEmail)

  override def isSasAppAllowed(userEmail: WorkbenchEmail)(implicit ev: Ask[F, TraceId]): F[Boolean] =
    samDao.isGroupMembersOrAdmin(config.sasAppCreationAllowedGroup, userEmail)

  override def isAdminUser(userInfo: UserInfo)(implicit ev: Ask[F, TraceId]): F[Boolean] =
    samDao.isAdminUser(userInfo)

}

final case class SamAuthProviderConfig(authCacheEnabled: Boolean,
                                       authCacheMaxSize: Int = 1000,
                                       authCacheExpiryTime: FiniteDuration = 15 minutes,
                                       customAppCreationAllowedGroup: GroupName,
                                       sasAppCreationAllowedGroup: GroupName
)

private[leonardo] case class AuthCacheKey(samResourceType: SamResourceType,
                                          samResource: String,
                                          authorization: Authorization,
                                          action: String
)
