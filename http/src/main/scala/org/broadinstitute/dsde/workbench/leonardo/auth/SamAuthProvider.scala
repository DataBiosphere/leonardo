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
  override def hasPermission[R, A](samResource: R, action: A, userInfo: UserInfo)(
    implicit sr: SamResourceAction[R, A],
    ev: Ask[F, TraceId]
  ): F[Boolean] = {
    val authHeader = Authorization(Credentials.Token(AuthScheme.Bearer, userInfo.accessToken.token))
    if (config.authCacheEnabled && sr.cacheableActions.contains(action)) {
      authCache.cachingF(
        AuthCacheKey(sr.resourceType, sr.resourceIdAsString(samResource), authHeader, sr.actionAsString(action))
      )(None) {
        checkPermission(sr.resourceType, sr.resourceIdAsString(samResource), sr.actionAsString(action), authHeader)
      }
    } else {
      checkPermission(sr.resourceType, sr.resourceIdAsString(samResource), sr.actionAsString(action), authHeader)
    }
  }

  private def checkPermission(samResourceType: SamResourceType,
                              samResource: String,
                              action: String,
                              authHeader: Authorization)(
    implicit ev: Ask[F, TraceId]
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
  )(implicit sr: SamResourceAction[R, A], ev: Ask[F, TraceId]): F[List[sr.ActionCategory]] = {
    val authorization = Authorization(Credentials.Token(AuthScheme.Bearer, userInfo.accessToken.token))
    for {
      listOfPermissions <- samDao
        .getListOfResourcePermissions[R, A](samResource, authorization)
      setOfPermissions = listOfPermissions.toSet
      callerActions = sr.allActions.collect { case a if setOfPermissions.contains(a) => a }
    } yield callerActions
  }

  def getActionsWithProjectFallback[R, A](samResource: R, googleProject: GoogleProject, userInfo: UserInfo)(
    implicit sr: SamResourceAction[R, A],
    ev: Ask[F, TraceId]
  ): F[(List[sr.ActionCategory], List[ProjectAction])] = {
    val authorization = Authorization(Credentials.Token(AuthScheme.Bearer, userInfo.accessToken.token))
    for {
      listOfPermissions <- samDao
        .getListOfResourcePermissions(samResource, authorization)
      setOfPermissions = listOfPermissions.toSet
      callerActions = sr.allActions.collect { case a if setOfPermissions.contains(a) => a }

      listOfProjectPermissions <- samDao.getListOfResourcePermissions(ProjectSamResourceId(googleProject),
                                                                      authorization)
      setOfProjectPermissions = listOfProjectPermissions.toSet
      projectCallerActions = ProjectAction.allActions.toList.collect {
        case a if setOfProjectPermissions.contains(a) => a
      }
    } yield (callerActions, projectCallerActions)
  }

  override def filterUserVisible[R](resources: NonEmptyList[R], userInfo: UserInfo)(
    implicit sr: SamResource[R],
    decoder: Decoder[R],
    ev: Ask[F, TraceId]
  ): F[List[R]] = {
    val authHeader = Authorization(Credentials.Token(AuthScheme.Bearer, userInfo.accessToken.token))
    for {
      resourcePolicies <- samDao
        .getResourcePolicies[R](authHeader)
      res = resourcePolicies.filter { case (_, pn) => sr.policyNames.contains(pn) }
    } yield resources.filter(r => res.exists(_._1 == r))
  }

  def filterUserVisibleWithProjectFallback[R](
    resources: NonEmptyList[(GoogleProject, R)],
    userInfo: UserInfo
  )(
    implicit sr: SamResource[R],
    decoder: Decoder[R],
    ev: Ask[F, TraceId]
  ): F[List[(GoogleProject, R)]] = {
    val authHeader = Authorization(Credentials.Token(AuthScheme.Bearer, userInfo.accessToken.token))
    for {
      projectPolicies <- samDao.getResourcePolicies[ProjectSamResourceId](authHeader)
      owningProjects = projectPolicies.collect {
        case (r, SamPolicyName.Owner) => r.googleProject
      }
      resourcePolicies <- samDao
        .getResourcePolicies[R](authHeader)
      res = resourcePolicies.filter { case (_, pn) => sr.policyNames.contains(pn) }
    } yield resources.filter {
      case (project, r) =>
        owningProjects.contains(project) || res.exists(_._1 == r)
    }
  }

  def filterUserVisibleWithWorkspaceFallback[R](
    resources: NonEmptyList[(WorkspaceId, R)],
    userInfo: UserInfo
  )(
    implicit sr: SamResource[R],
    decoder: Decoder[R],
    ev: Ask[F, TraceId]
  ): F[List[(WorkspaceId, R)]] = {
    val authHeader = Authorization(Credentials.Token(AuthScheme.Bearer, userInfo.accessToken.token))
    for {
      workspacePolicies <- samDao.getResourcePolicies[WorkspaceResourceSamResourceId](authHeader)
      owningWorkspaces = workspacePolicies.collect {
        case (r, SamPolicyName.Owner) => r.workspaceId
      }
      resourcePolicies <- samDao
        .getResourcePolicies[R](authHeader)
      res = resourcePolicies.filter { case (_, pn) => sr.policyNames.contains(pn) }
    } yield resources.filter {
      case (id, r) =>
        owningWorkspaces.contains(id) || res.exists(_._1 == r)
    }
  }

  def isUserWorkspaceOwner[R](
    workspaceId: WorkspaceId,
    workspaceResource: R,
    userInfo: UserInfo
  )(implicit sr: SamResource[R], decoder: Decoder[R], ev: Ask[F, TraceId]): F[Boolean] = {
    val authHeader = Authorization(Credentials.Token(AuthScheme.Bearer, userInfo.accessToken.token))
    for {
      workspacePolicies <- samDao.getResourcePolicies[WorkspaceResourceSamResourceId](authHeader)
      owningWorkspaces = workspacePolicies.collect {
        case (r, SamPolicyName.Owner) => r.workspaceId
      }
      resourcePolicies <- samDao
        .getResourcePolicies[R](authHeader)
      res = resourcePolicies.filter { case (_, pn) => sr.policyNames.contains(pn) }
    } yield owningWorkspaces.contains(workspaceId) || res.exists(_._1 == workspaceResource)
  }

  override def notifyResourceCreated[R](
    samResource: R,
    creatorEmail: WorkbenchEmail,
    googleProject: GoogleProject
  )(implicit sr: SamResource[R], encoder: Encoder[R], ev: Ask[F, TraceId]): F[Unit] =
    // TODO: consider using v2 for all existing entities if this works out for apps https://broadworkbench.atlassian.net/browse/IA-2569
    // Apps are modeled different in SAM than other leo resources.
    if (sr.resourceType != SamResourceType.App)
      samDao.createResource(samResource, creatorEmail, googleProject)
    else
      samDao.createResourceWithParent(samResource, creatorEmail, googleProject)

  override def notifyResourceDeleted[R](
    samResource: R,
    creatorEmail: WorkbenchEmail,
    googleProject: GoogleProject
  )(implicit sr: SamResource[R], ev: Ask[F, TraceId]): F[Unit] =
    samDao.deleteResource(samResource, creatorEmail, googleProject)

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
      _ <- if (samUserInfo.enabled) F.unit
      else
        F.raiseError(
          AuthProviderException(
            traceId,
            s"[SamAuthProvider.lookupOriginatingUserEmail] User ${samUserInfo.userEmail.value} is disabled.",
            StatusCodes.Forbidden
          )
        )
    } yield samUserInfo.userEmail

  override def isCustomAppAllowed(userEmail: WorkbenchEmail)(implicit ev: Ask[F, TraceId]): F[Boolean] =
    samDao.isGroupMembersOrAdmin(config.customAppCreationAllowedGroup, userEmail)

}

final case class SamAuthProviderConfig(authCacheEnabled: Boolean,
                                       authCacheMaxSize: Int = 1000,
                                       authCacheExpiryTime: FiniteDuration = 15 minutes,
                                       customAppCreationAllowedGroup: GroupName)

private[leonardo] case class AuthCacheKey(samResourceType: SamResourceType,
                                          samResource: String,
                                          authorization: Authorization,
                                          action: String)
