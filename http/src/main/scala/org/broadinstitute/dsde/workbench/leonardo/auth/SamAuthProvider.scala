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
import org.broadinstitute.dsde.workbench.leonardo.dao.{
  AuthProviderException,
  GetResourceParentResponse,
  GroupName,
  SamDAO
}
import org.broadinstitute.dsde.workbench.leonardo.model._
import org.broadinstitute.dsde.workbench.model.google.GoogleProject
import org.broadinstitute.dsde.workbench.model.{TraceId, UserInfo, WorkbenchEmail}
import org.broadinstitute.dsde.workbench.openTelemetry.OpenTelemetryMetrics
import org.http4s.client.dsl.Http4sClientDsl
import org.http4s.headers.Authorization
import org.http4s.{AuthScheme, Credentials}
import org.typelevel.log4cats.StructuredLogger
import scalacache.Cache

import java.util.UUID
import scala.concurrent.duration._

class SamAuthProvider[F[_]: OpenTelemetryMetrics](
  samDao: SamDAO[F],
  config: SamAuthProviderConfig,
  saProvider: ServiceAccountProvider[F],
  authCache: Cache[F, AuthCacheKey, Boolean]
)(implicit F: Async[F], logger: StructuredLogger[F], metrics: OpenTelemetryMetrics[F])
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
   * Get all Sam resource IDs of the given type on which the user is granted permission to `discover`. Returns ids with explicitly
   * granted roles only; users with workspace `owner` role, for example, can see all runtimes in the workspace, but
   * `listResourceIds[SamRuntimeResourceId]` is not guaranteed to return all runtimes in that workspace. Works for any
   * Sam resource type except SharedApp and similar alternative app types; these require a specific Sam resource object
   * to determine permissions, as these are stored on the individual records.
   * @param hasOwnerRole whether to return only resources that the user is granted ownership on (has role = `ownerRoleName`)
   * @param userInfo the current user
   * @param resourceDefinition the resource archetype (App, Runtime, etc)
   * @param resourceIdDecoder converts Json -> SamResourceId, needed by SamDao
   * @param ev the request context
   * @tparam R SamResourceId
   * @return List[SamResourceId] of all resources of type R which are discoverable (or owned, if `hasOwnerRole`) by the user
   */
  override def listResourceIds[R <: SamResourceId](
    hasOwnerRole: Boolean,
    userInfo: UserInfo
  )(implicit
    resourceDefinition: SamResource[R],
    appDefinition: SamResource[AppSamResourceId],
    resourceIdDecoder: Decoder[R],
    ev: Ask[F, TraceId]
  ): F[Set[R]] = {
    val authHeader = Authorization(Credentials.Token(AuthScheme.Bearer, userInfo.accessToken.token))

    // TODO model these permissions in Sam, so we can just get the ids on which user is granted Discover action
    val ownerRole: SamRole = resourceDefinition.ownerRoleName

    for {
      traceId <- ev.ask
      _ <- F.raiseWhen(
        resourceDefinition == appDefinition
      )(
        AuthProviderException(
          traceId,
          s"SamAuthProvider.listResourceIds should not be called for App resources",
          StatusCodes.BadRequest
        )
      )

      resourcesWithRole: List[(R, SamRole)] <- samDao
        .listResourceIdsWithRole[R](authHeader)

      discoverableIds: Set[R] = resourcesWithRole
        .filter {
          case (_, role) if hasOwnerRole => role == ownerRole
          case _                         => true
        }
        .map { case (samResourceId, _) =>
          samResourceId
        }
        .toSet
    } yield discoverableIds
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

  override def getLeoAuthToken: F[String] =
    for {
      auth <- samDao.getLeoAuthToken
      token <- auth.credentials match {
        case org.http4s.Credentials.Token(_, token) => F.pure(token)
        case _ => F.raiseError(new RuntimeException("Could not obtain Leo auth token"))
      }
    } yield token

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
   * @param ev trace id
   * @return optional workspace ID
   */
  override def lookupWorkspaceParentForGoogleProject(userInfo: UserInfo, googleProject: GoogleProject)(implicit
    ev: Ask[F, TraceId]
  ): F[Option[WorkspaceId]] = for {
    traceId <- ev.ask

    // Get resource parent from Sam
    authHeader = Authorization(Credentials.Token(AuthScheme.Bearer, userInfo.accessToken.token))
    parent <- samDao
      .getResourceParent(authHeader, ProjectSamResourceId(googleProject))

    // Annotate error cases but don't fail
    workspaceId = parent match {
      case Some(GetResourceParentResponse(SamResourceType.Workspace, resourceId)) =>
        Either.catchNonFatal(UUID.fromString(resourceId)).map(WorkspaceId).leftMap(_.getMessage)
      case Some(_) => Left(s"Unexpected parent type $parent for google project $googleProject")
      case None    => Left(s"Parent not found in Sam for google project $googleProject")
    }

    // Log result and emit metric
    metricName = "lookupWorkspace"
    _ <- workspaceId match {
      case Right(res) =>
        logger.info(Map("traceId" -> traceId.asString))(
          s"Populating parent workspace ID $res for google project $googleProject"
        ) >> metrics.incrementCounter(metricName, tags = Map("succeeded" -> "true"))
      case Left(error) =>
        logger.warn(Map("traceId" -> traceId.asString))(
          s"Unable to populate workspace ID for google project $googleProject: $error"
        ) >> metrics.incrementCounter(metricName, tags = Map("succeeded" -> "false"))
    }
  } yield workspaceId.toOption

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
