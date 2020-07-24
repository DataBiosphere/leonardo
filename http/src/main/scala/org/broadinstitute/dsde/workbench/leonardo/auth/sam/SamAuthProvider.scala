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
import io.circe.Decoder
import org.http4s.circe.CirceEntityDecoder._
import org.broadinstitute.dsde.workbench.leonardo.SamResource.ProjectSamResource
import org.broadinstitute.dsde.workbench.leonardo.SamResourcePolicy.SamProjectPolicy
import org.broadinstitute.dsde.workbench.leonardo.dao.SamDAO
import org.broadinstitute.dsde.workbench.leonardo.dao.HttpSamDAO._
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
    implicit ev: ApplicativeAsk[F, TraceId],
    ev2: AuthCheckable[R]
  ): F[Boolean] = {
    val authHeader = Authorization(Credentials.Token(AuthScheme.Bearer, userInfo.accessToken.token))
    if (config.authCacheEnabled && ev2.cacheableActions.contains(action)) {
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
  )(implicit ev: ApplicativeAsk[F, TraceId], ev2: AuthCheckable[R]): F[Boolean] = {
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
  )(implicit ev: ApplicativeAsk[F, TraceId], ev2: AuthCheckable[R]): F[List[LeoAuthAction]] = {
    val authorization = Authorization(Credentials.Token(AuthScheme.Bearer, userInfo.accessToken.token))
    for {
      listOfPermissions <- samDao
        .getListOfResourcePermissions(samResource, authorization)
      callerActions = ev2.actions.collect { case a if listOfPermissions.contains(a.asString) => a }
    } yield callerActions.toList
  }

  def getActionsWithProjectFallback[R <: SamResource](samResource: R, googleProject: GoogleProject, userInfo: UserInfo)(
    implicit ev: ApplicativeAsk[F, TraceId],
    ev2: AuthCheckable[R]
  ): F[List[LeoAuthAction]] = {
    val authorization = Authorization(Credentials.Token(AuthScheme.Bearer, userInfo.accessToken.token))
    for {
      listOfPermissions <- samDao
        .getListOfResourcePermissions(samResource, authorization)
      callerActions = ev2.actions.collect { case a if listOfPermissions.contains(a.asString) => a }

      listOfProjectPermissions <- samDao.getListOfResourcePermissions(ProjectSamResource(googleProject), authorization)
      projectCallerActions = AuthCheckable.ProjectAuthCheckable.actions.collect {
        case a if listOfProjectPermissions.contains(a.toString) => a
      }
    } yield callerActions ++ projectCallerActions
  }

  override def filterUserVisible[R <: SamResource](resources: List[R], userInfo: UserInfo)(
    implicit ev: ApplicativeAsk[F, TraceId],
    ev2: AuthCheckable[R]
  ): F[List[R]] = {
    val authHeader = Authorization(Credentials.Token(AuthScheme.Bearer, userInfo.accessToken.token))
    implicit val decoder: Decoder[ev2.Policy] = ev2.policyDecoder
    for {
      resourcePolicies <- samDao
        .getResourcePolicies[ev2.Policy](authHeader, ev2.resourceType)
      res = resourcePolicies.filter(rp => ev2.policyNames.contains(rp.accessPolicyName))
    } yield resources.filter(r => res.exists(_.resource == r))
  }

  def filterUserVisibleWithProjectFallback[R <: SamResource](
    resources: List[(GoogleProject, R)],
    userInfo: UserInfo
  )(
    implicit ev: ApplicativeAsk[F, TraceId],
    ev2: AuthCheckable[R]
  ): F[List[(GoogleProject, R)]] = {
    val authHeader = Authorization(Credentials.Token(AuthScheme.Bearer, userInfo.accessToken.token))
    implicit val decoder: Decoder[ev2.Policy] = ev2.policyDecoder
    for {
      projectPolicies <- samDao.getResourcePolicies[SamProjectPolicy](authHeader, SamResourceType.Project)
      owningProjects = projectPolicies.collect {
        case x if x.accessPolicyName == AccessPolicyName.Owner => x.resource.googleProject
      }
      resourcePolicies <- samDao
        .getResourcePolicies[ev2.Policy](authHeader, ev2.resourceType)
      res = resourcePolicies.filter(rp => ev2.policyNames.contains(rp.accessPolicyName))
    } yield resources.filter {
      case (project, r) =>
        owningProjects.contains(project) || res.exists(_.resource == r)
    }
  }

  override def notifyResourceCreated(samResource: SamResource,
                                     creatorEmail: WorkbenchEmail,
                                     googleProject: GoogleProject)(implicit ev: ApplicativeAsk[F, TraceId]): F[Unit] =
    samDao.createResource(samResource, creatorEmail, googleProject)

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
