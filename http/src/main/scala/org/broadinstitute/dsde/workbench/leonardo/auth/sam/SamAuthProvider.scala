package org.broadinstitute.dsde.workbench.leonardo
package auth.sam

import java.util.UUID
import java.util.concurrent.TimeUnit

import cats.effect.implicits._
import cats.effect.{Blocker, ContextShift, Effect, Sync}
import cats.implicits._
import cats.mtl.ApplicativeAsk
import com.google.common.cache.{CacheBuilder, CacheLoader}
import io.chrisdavenport.log4cats.Logger
import org.broadinstitute.dsde.workbench.leonardo.SamResource.ProjectSamResource
import org.broadinstitute.dsde.workbench.leonardo.dao.SamDAO
import org.broadinstitute.dsde.workbench.leonardo.model._
import org.broadinstitute.dsde.workbench.model.google.GoogleProject
import org.broadinstitute.dsde.workbench.model.{TraceId, UserInfo, WorkbenchEmail}
import org.http4s.client.dsl.Http4sClientDsl
import org.http4s.headers.Authorization
import org.http4s.{AuthScheme, Credentials}

import scala.concurrent.duration._

class SamAuthProvider[F[_]: Effect: Logger](samDao: SamDAO[F],
                                            config: SamAuthProviderConfig,
                                            saProvider: ServiceAccountProvider[F],
                                            blocker: Blocker)(implicit cs: ContextShift[F])
    extends LeoAuthProvider[F]
    with Http4sClientDsl[F] {
  override def serviceAccountProvider: ServiceAccountProvider[F] = saProvider

  // We cache some auth results from Sam which are requested very often by the proxy, such as
  // "connect to runtime" and "connect to app".
  private[sam] val authCache = CacheBuilder
    .newBuilder()
    .expireAfterWrite(config.authCacheExpiryTime.toSeconds, TimeUnit.SECONDS)
    .maximumSize(config.authCacheMaxSize)
    .build(
      new CacheLoader[AuthCacheKey, java.lang.Boolean] {
        override def load(key: AuthCacheKey): java.lang.Boolean = {
          implicit val traceId = ApplicativeAsk.const[F, TraceId](TraceId(UUID.randomUUID()))
          checkPermission(key.samResource, key.action, key.authorization).toIO
            .unsafeRunSync()
        }
      }
    )

  override def hasPermission[R <: SamResource, A <: LeoAuthAction](samResource: R, action: A, userInfo: UserInfo)(
    implicit act: ActionCheckable[R, A],
    ev: ApplicativeAsk[F, TraceId]
  ): F[Boolean] = {
    val authHeader = Authorization(Credentials.Token(AuthScheme.Bearer, userInfo.accessToken.token))
    if (config.authCacheEnabled && act.cacheableActions.contains(action)) {
      blocker.blockOn(
        Effect[F].delay(
          authCache
            .get(AuthCacheKey(samResource, authHeader, action))
            .booleanValue()
        )
      )
    } else {
      checkPermission(samResource, action, authHeader)
    }
  }

  private def checkPermission[R <: SamResource](samResource: R, action: LeoAuthAction, authHeader: Authorization)(
    implicit ev: ApplicativeAsk[F, TraceId]
  ): F[Boolean] =
    for {
      traceId <- ev.ask
      res <- samDao.hasResourcePermission(samResource, action.asString, authHeader).recoverWith {
        case e =>
          Logger[F]
            .info(e)(s"${traceId} | $action is not allowed for resource $samResource")
            .as(false)
      }
    } yield res

  override def hasPermissionWithProjectFallback[R <: SamResource, A <: LeoAuthAction](
    samResource: R,
    action: A,
    projectAction: ProjectAction,
    userInfo: UserInfo,
    googleProject: GoogleProject
  )(implicit act: ActionCheckable[R, A], ev: ApplicativeAsk[F, TraceId]): F[Boolean] = {
    val authHeader = Authorization(Credentials.Token(AuthScheme.Bearer, userInfo.accessToken.token))
    for {
      // First check permission at the resource level
      resourcePermission <- hasPermission(samResource, action, userInfo)
      // Fall back to the project-level check if necessary
      res <- resourcePermission match {
        case true => Sync[F].pure(true)
        case _    => hasPermission(ProjectSamResource(googleProject), projectAction, userInfo)
      }
    } yield res
  }

  override def getActions[R <: SamResource, A <: LeoAuthAction](
    samResource: R,
    userInfo: UserInfo
  )(implicit act: ActionCheckable[R, A], ev: ApplicativeAsk[F, TraceId]): F[List[act.ActionCategory]] = {
    val authorization = Authorization(Credentials.Token(AuthScheme.Bearer, userInfo.accessToken.token))
    for {
      listOfPermissions <- samDao
        .getListOfResourcePermissions(samResource, authorization)
      setOfPermissions = listOfPermissions.toSet
      callerActions = act.allActions.collect { case a if listOfPermissions.contains(a.asString) => a }
    } yield callerActions
  }

  def getActionsWithProjectFallback[R <: SamResource, A <: LeoAuthAction](samResource: R,
                                                                          googleProject: GoogleProject,
                                                                          userInfo: UserInfo)(
    implicit act: ActionCheckable[R, A],
    ev: ApplicativeAsk[F, TraceId]
  ): F[List[LeoAuthAction]] = {
    val authorization = Authorization(Credentials.Token(AuthScheme.Bearer, userInfo.accessToken.token))
    for {
      listOfPermissions <- samDao
        .getListOfResourcePermissions(samResource, authorization)
      setOfPermissions = listOfPermissions.toSet
      callerActions = act.allActions.collect { case a if setOfPermissions.contains(a.asString) => a }

      listOfProjectPermissions <- samDao.getListOfResourcePermissions(ProjectSamResource(googleProject), authorization)
      setOfProjectPermissions = listOfProjectPermissions.toSet
      projectCallerActions = ProjectAction.allActions.collect {
        case a if setOfProjectPermissions.contains(a.asString) => a
      }
    } yield callerActions ++ projectCallerActions
  }

  override def filterUserVisible[R <: SamResource](resources: List[R], userInfo: UserInfo)(
    implicit pol: PolicyCheckable[R],
    ev: ApplicativeAsk[F, TraceId]
  ): F[List[R]] = {
    val authHeader = Authorization(Credentials.Token(AuthScheme.Bearer, userInfo.accessToken.token))
    for {
      resourcePolicies <- samDao
        .getResourcePolicies(authHeader, pol.resourceType)
      res = resourcePolicies.filter(rp => pol.policyNames.contains(rp.policyName))
    } yield resources.filter(r => res.exists(_.samResource == r))
  }

  def filterUserVisibleWithProjectFallback[R <: SamResource](
    resources: List[(GoogleProject, R)],
    userInfo: UserInfo
  )(
    implicit pol: PolicyCheckable[R],
    ev: ApplicativeAsk[F, TraceId]
  ): F[List[(GoogleProject, R)]] = {
    val authHeader = Authorization(Credentials.Token(AuthScheme.Bearer, userInfo.accessToken.token))
    for {
      projectPolicies <- samDao.getResourcePolicies(authHeader, SamResourceType.Project)
      owningProjects = projectPolicies.collect {
        case SamResourcePolicy(ProjectSamResource(p), SamPolicyName.Owner) => p
      }
      resourcePolicies <- samDao
        .getResourcePolicies(authHeader, pol.resourceType)
      res = resourcePolicies.filter(rp => pol.policyNames.contains(rp.policyName))
    } yield resources.filter {
      case (project, r) =>
        owningProjects.contains(project) || res.exists(_.samResource == r)
    }
  }

  override def notifyResourceCreated[R <: SamResource](
    samResource: R,
    creatorEmail: WorkbenchEmail,
    googleProject: GoogleProject
  )(implicit pol: PolicyCheckable[R], ev: ApplicativeAsk[F, TraceId]): F[Unit] =
    if (pol.policyNames == Set(SamPolicyName.Creator))
      samDao.createResource(samResource, creatorEmail, googleProject)
    else
      samDao.createResourceWithManagerPolicy(samResource, creatorEmail, googleProject)

  override def notifyResourceDeleted[R <: SamResource](
    samResource: R,
    creatorEmail: WorkbenchEmail,
    googleProject: GoogleProject
  )(implicit pol: PolicyCheckable[R], ev: ApplicativeAsk[F, TraceId]): F[Unit] =
    samDao.deleteResource(samResource, creatorEmail, googleProject)

}

final case class SamAuthProviderConfig(authCacheEnabled: Boolean,
                                       authCacheMaxSize: Int = 1000,
                                       authCacheExpiryTime: FiniteDuration = 15 minutes)

private[sam] case class AuthCacheKey(samResource: SamResource, authorization: Authorization, action: LeoAuthAction)
