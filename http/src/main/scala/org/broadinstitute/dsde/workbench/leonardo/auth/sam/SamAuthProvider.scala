package org.broadinstitute.dsde.workbench.leonardo
package auth.sam

import java.util.UUID
import java.util.concurrent.TimeUnit

import akka.http.scaladsl.model.StatusCodes
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

case class UnknownLeoAuthAction(msg: String) extends LeoException(msg, StatusCodes.InternalServerError)

class SamAuthProvider[F[_]: Effect: Logger](samDao: SamDAO[F],
                                            config: SamAuthProviderConfig,
                                            saProvider: ServiceAccountProvider[F],
                                            blocker: Blocker)(implicit cs: ContextShift[F])
    extends LeoAuthProvider[F]
    with Http4sClientDsl[F] {
  override def serviceAccountProvider: ServiceAccountProvider[F] = saProvider

  // We cache some auth results from Sam which are requested very often by the project, such as
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

  override def hasPermission[R <: SamResource](samResource: R, action: LeoAuthAction, userInfo: UserInfo)(
    implicit authConfig: AuthCheckable[R],
    ev: ApplicativeAsk[F, TraceId]
  ): F[Boolean] = {
    val authHeader = Authorization(Credentials.Token(AuthScheme.Bearer, userInfo.accessToken.token))
    if (config.authCacheEnabled && authConfig.cacheableActions.contains(action)) {
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

  override def hasPermissionWithProjectFallback[R <: SamResource](
    samResource: R,
    action: LeoAuthAction,
    projectAction: ProjectAction,
    userInfo: UserInfo,
    googleProject: GoogleProject
  )(implicit authConfig: AuthCheckable[R], ev: ApplicativeAsk[F, TraceId]): F[Boolean] = {
    val authHeader = Authorization(Credentials.Token(AuthScheme.Bearer, userInfo.accessToken.token))
    for {
      // First check permission at the resource level
      hasPermission <- hasPermission(samResource, action, userInfo)
      // Fall back to the project-level check if necessary
      res <- hasPermission match {
        case true => Sync[F].pure(true)
        case _    => checkPermission(ProjectSamResource(googleProject), projectAction, authHeader)
      }
    } yield res
  }

  override def getActions[R <: SamResource](
    samResource: R,
    userInfo: UserInfo
  )(implicit authConfig: AuthCheckable[R], ev: ApplicativeAsk[F, TraceId]): F[List[LeoAuthAction]] = {
    val authorization = Authorization(Credentials.Token(AuthScheme.Bearer, userInfo.accessToken.token))
    for {
      listOfPermissions <- samDao
        .getListOfResourcePermissions(samResource, authorization)
      callerActions = authConfig.actions.collect { case a if listOfPermissions.contains(a.asString) => a }
    } yield callerActions.toList
  }

  def getActionsWithProjectFallback[R <: SamResource](samResource: R, googleProject: GoogleProject, userInfo: UserInfo)(
    implicit authConfig: AuthCheckable[R],
    ev: ApplicativeAsk[F, TraceId]
  ): F[List[LeoAuthAction]] = {
    val authorization = Authorization(Credentials.Token(AuthScheme.Bearer, userInfo.accessToken.token))
    for {
      listOfPermissions <- samDao
        .getListOfResourcePermissions(samResource, authorization)
      callerActions = authConfig.actions.toList.collect { case a if listOfPermissions.contains(a.asString) => a }

      listOfProjectPermissions <- samDao.getListOfResourcePermissions(ProjectSamResource(googleProject), authorization)
      projectCallerActions = AuthCheckable.ProjectAuthCheckable.actions.toList.collect {
        case a if listOfProjectPermissions.contains(a.toString) => a
      }
    } yield callerActions ++ projectCallerActions
  }

  override def filterUserVisible[R <: SamResource](resources: List[R], userInfo: UserInfo)(
    implicit authConfig: AuthCheckable[R],
    ev: ApplicativeAsk[F, TraceId]
  ): F[List[R]] = {
    val authHeader = Authorization(Credentials.Token(AuthScheme.Bearer, userInfo.accessToken.token))
    for {
      resourcePolicies <- samDao
        .getResourcePolicies(authHeader, authConfig.resourceType)
      res = resourcePolicies.filter(rp => authConfig.policyNames.contains(rp.policyName))
    } yield resources.filter(r => res.exists(_.samResource == r))
  }

  def filterUserVisibleWithProjectFallback[R <: SamResource](
    resources: List[(GoogleProject, R)],
    userInfo: UserInfo
  )(
    implicit authConfig: AuthCheckable[R],
    ev: ApplicativeAsk[F, TraceId]
  ): F[List[(GoogleProject, R)]] = {
    val authHeader = Authorization(Credentials.Token(AuthScheme.Bearer, userInfo.accessToken.token))
    for {
      projectPolicies <- samDao.getResourcePolicies(authHeader, SamResourceType.Project)
      owningProjects = projectPolicies.collect {
        case SamResourcePolicy(ProjectSamResource(p), AccessPolicyName.Owner) => p
      }
      resourcePolicies <- samDao
        .getResourcePolicies(authHeader, authConfig.resourceType)
      res = resourcePolicies.filter(rp => authConfig.policyNames.contains(rp.policyName))
    } yield resources.filter {
      case (project, r) =>
        owningProjects.contains(project) || res.exists(_.samResource == r)
    }
  }

  override def notifyResourceCreated(samResource: SamResource,
                                     creatorEmail: WorkbenchEmail,
                                     googleProject: GoogleProject,
                                     createManagerPolicy: Boolean)(implicit ev: ApplicativeAsk[F, TraceId]): F[Unit] =
    samDao.createResource(samResource, creatorEmail, googleProject, createManagerPolicy)

  override def notifyResourceDeleted(samResource: SamResource,
                                     userEmail: WorkbenchEmail,
                                     creatorEmail: WorkbenchEmail,
                                     googleProject: GoogleProject)(implicit ev: ApplicativeAsk[F, TraceId]): F[Unit] =
    samDao.deleteResource(samResource, userEmail, creatorEmail, googleProject)

}

final case class SamAuthProviderConfig(authCacheEnabled: Boolean,
                                       authCacheMaxSize: Int = 1000,
                                       authCacheExpiryTime: FiniteDuration = 15 minutes)

private[sam] case class AuthCacheKey(samResource: SamResource, authorization: Authorization, action: LeoAuthAction)
