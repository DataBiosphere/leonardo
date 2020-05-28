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
import org.broadinstitute.dsde.workbench.leonardo.SamResource.{
  PersistentDiskSamResource,
  ProjectSamResource,
  RuntimeSamResource
}
import org.broadinstitute.dsde.workbench.leonardo.dao.HttpSamDAO._
import org.broadinstitute.dsde.workbench.leonardo.dao.SamDAO
import org.broadinstitute.dsde.workbench.leonardo.model.ProjectAction.{CreatePersistentDisk, CreateRuntime}
import org.broadinstitute.dsde.workbench.leonardo.model.RuntimeAction._
import org.broadinstitute.dsde.workbench.leonardo.model.PersistentDiskAction._
import org.broadinstitute.dsde.workbench.leonardo.model._
import org.broadinstitute.dsde.workbench.model.google.GoogleProject
import org.broadinstitute.dsde.workbench.model.{TraceId, UserInfo, WorkbenchEmail}
import org.http4s.circe.CirceEntityDecoder._
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

  // Cache notebook auth results from Sam as this is called very often by the proxy and the "list runtimes" endpoint.
  // Project-level auth is not cached because it's just called for "create runtime" which is not as frequent.
  private[sam] val notebookAuthCache = CacheBuilder
    .newBuilder()
    .expireAfterWrite(config.notebookAuthCacheExpiryTime.toSeconds, TimeUnit.SECONDS)
    .maximumSize(config.notebookAuthCacheMaxSize)
    .build(
      new CacheLoader[NotebookAuthCacheKey, java.lang.Boolean] {
        override def load(key: NotebookAuthCacheKey): java.lang.Boolean = {
          implicit val traceId = ApplicativeAsk.const[F, TraceId](TraceId(UUID.randomUUID()))
          checkRuntimePermissionWithProjectFallback(key.samResource, key.authorization, key.action, key.googleProject).toIO
            .unsafeRunSync()
        }
      }
    )

  private def getProjectActionString(action: LeoAuthAction): Either[Throwable, String] =
    projectActionMap
      .get(action)
      .toRight(
        UnknownLeoAuthAction(
          s"SamAuthProvider has no mapping for project authorization action ${action.toString}, and is therefore probably out of date."
        )
      )

  private def getNotebookActionString(action: LeoAuthAction): Either[Throwable, String] =
    runtimeActionMap
      .get(action)
      .toRight(
        UnknownLeoAuthAction(
          s"SamAuthProvider has no mapping for notebook-cluster authorization action ${action.toString}, and is therefore probably out of date."
        )
      )

  private def getPersistentDiskActionString(action: LeoAuthAction): Either[Throwable, String] =
    persistentDiskActionMap
      .get(action)
      .toRight(
        UnknownLeoAuthAction(
          s"SamAuthProvider has no mapping for persistent-disk authorization action ${action.toString}, and is therefore probably out of date."
        )
      )

  private val projectActionMap: Map[LeoAuthAction, String] = Map(
    GetRuntimeStatus -> "list_notebook_cluster",
    CreateRuntime -> "launch_notebook_cluster",
    SyncDataToRuntime -> "sync_notebook_cluster",
    DeleteRuntime -> "delete_notebook_cluster",
    StopStartRuntime -> "stop_start_notebook_cluster",
    CreatePersistentDisk -> "create_persistent_disk",
    DeletePersistentDisk -> "delete_persistent_disk"
  )

  private val runtimeActionMap: Map[LeoAuthAction, String] = Map(
    GetRuntimeStatus -> "status",
    ConnectToRuntime -> "connect",
    SyncDataToRuntime -> "sync",
    DeleteRuntime -> "delete",
    ModifyRuntime -> "modify",
    StopStartRuntime -> "stop_start"
  )

  private val persistentDiskActionMap: Map[LeoAuthAction, String] = Map(
    ReadPersistentDisk -> "read",
    AttachPersistentDisk -> "attach",
    ModifyPersistentDisk -> "modify",
    DeletePersistentDisk -> "delete"
  )

  override def hasProjectPermission(
    userInfo: UserInfo,
    action: ProjectAction,
    googleProject: GoogleProject
  )(implicit ev: ApplicativeAsk[F, TraceId]): F[Boolean] = {
    val authHeader = Authorization(Credentials.Token(AuthScheme.Bearer, userInfo.accessToken.token))
    hasProjectPermissionInternal(googleProject, action, authHeader)
  }

  override def hasRuntimePermission(
    samResource: RuntimeSamResource,
    userInfo: UserInfo,
    action: RuntimeAction,
    googleProject: GoogleProject
  )(implicit ev: ApplicativeAsk[F, TraceId]): F[Boolean] = {
    val authorization = Authorization(Credentials.Token(AuthScheme.Bearer, userInfo.accessToken.token))
    // Consult the notebook auth cache if enabled
    if (config.notebookAuthCacheEnabled) {
      // tokenExpiresIn should not taken into account when comparing cache keys
      blocker.blockOn(
        Effect[F].delay(
          notebookAuthCache
            .get(NotebookAuthCacheKey(samResource, authorization, action, googleProject))
            .booleanValue()
        )
      )
    } else {
      checkRuntimePermissionWithProjectFallback(samResource, authorization, action, googleProject)
    }
  }

  override def hasPersistentDiskPermission(
    samResource: PersistentDiskSamResource,
    userInfo: UserInfo,
    action: PersistentDiskAction,
    googleProject: GoogleProject
  )(implicit ev: ApplicativeAsk[F, TraceId]): F[Boolean] = {
    val authorization = Authorization(Credentials.Token(AuthScheme.Bearer, userInfo.accessToken.token))
    // can add check to cache here if necessary
    checkPersistentDiskPermissionWithProjectFallback(samResource, authorization, action, googleProject)
  }

  private def convertNotebookClusterActionsToString(action: String): NotebookClusterAction =
    action match {
      case "connect"    => NotebookClusterAction.ConnectToCluster
      case "modify"     => NotebookClusterAction.ModifyCluster
      case "sync"       => NotebookClusterAction.SyncDataToCluster
      case "stop_start" => NotebookClusterAction.StopStartCluster
      case "delete"     => NotebookClusterAction.DeleteCluster
    }

  def getNotebookClusterActions(internalId: RuntimeInternalId, userInfo: UserInfo)(
    implicit ev: ApplicativeAsk[F, TraceId]
  ): F[List[NotebookClusterAction]] = {

    val authorization = Authorization(Credentials.Token(AuthScheme.Bearer, userInfo.accessToken.token))

    for {
      listOfPermissions <- samDao
        .getListOfResourcePermissions(internalId.asString, ResourceTypeName.NotebookCluster, authorization)

    } yield {
      listOfPermissions.map(x => convertNotebookClusterActionsToString(x))
    }
  }

  private def checkRuntimePermissionWithProjectFallback(
                                                         samResource: RuntimeSamResource,
                                                         authorization: Authorization,
                                                         action: RuntimeAction,
                                                         googleProject: GoogleProject
  )(implicit ev: ApplicativeAsk[F, TraceId]): F[Boolean] =
    for {
      traceId <- ev.ask
      hasRuntimeAction <- hasRuntimePermissionInternal(samResource, action, authorization)
      res <- if (RuntimeAction.projectFallbackIneligibleActions contains action) Sync[F].pure(hasRuntimeAction)
      else {
        if (hasRuntimeAction)
          Sync[F].pure(true)
        else
          hasProjectPermissionInternal(googleProject, action, authorization).recoverWith {
            case e =>
              Logger[F]
                .info(e)(s"${traceId} | $action is not allowed at notebook-cluster level, nor project level")
                .as(false)
          }
      }
    } yield res

  private def checkPersistentDiskPermissionWithProjectFallback(
    samResource: PersistentDiskSamResource,
    authorization: Authorization,
    action: PersistentDiskAction,
    googleProject: GoogleProject
  )(implicit ev: ApplicativeAsk[F, TraceId]): F[Boolean] =
    for {
      traceId <- ev.ask
      hasPersistentDiskAction <- hasPersistentDiskPermissionInternal(samResource, action, authorization)
      res <- if (PersistentDiskAction.projectFallbackIneligibleActions contains action)
        Sync[F].pure(hasPersistentDiskAction)
      else {
        if (hasPersistentDiskAction)
          Sync[F].pure(true)
        else
          hasProjectPermissionInternal(googleProject, action, authorization).recoverWith {
            case e =>
              Logger[F]
                .info(e)(s"${traceId} | $action is not allowed at persistent-disk level, nor project level")
                .as(false)
          }
      }
    } yield res

  private def hasProjectPermissionInternal(
    googleProject: GoogleProject,
    action: LeoAuthAction,
    authHeader: Authorization
  )(implicit ev: ApplicativeAsk[F, TraceId]): F[Boolean] =
    for {
      actionString <- Effect[F].fromEither(getProjectActionString(action))
      res <- samDao.hasResourcePermission(ProjectSamResource(googleProject), actionString, authHeader)
    } yield res

  private def hasRuntimePermissionInternal(
    samResource: RuntimeSamResource,
    action: LeoAuthAction,
    authHeader: Authorization
  )(implicit ev: ApplicativeAsk[F, TraceId]): F[Boolean] =
    for {
      actionString <- Effect[F].fromEither(getNotebookActionString(action))
      res <- samDao.hasResourcePermission(samResource, actionString, authHeader)
    } yield res

  private def hasPersistentDiskPermissionInternal(
    samResource: PersistentDiskSamResource,
    action: LeoAuthAction,
    authHeader: Authorization
  )(implicit ev: ApplicativeAsk[F, TraceId]): F[Boolean] =
    for {
      actionString <- Effect[F].fromEither(getPersistentDiskActionString(action))
      res <- samDao.hasResourcePermission(samResource, actionString, authHeader)
    } yield res

  override def filterUserVisibleRuntimes(userInfo: UserInfo, runtimes: List[(GoogleProject, RuntimeSamResource)])(
    implicit ev: ApplicativeAsk[F, TraceId]
  ): F[List[(GoogleProject, RuntimeSamResource)]] = {
    val authHeader = Authorization(Credentials.Token(AuthScheme.Bearer, userInfo.accessToken.token))
    for {
      projectPolicies <- samDao.getResourcePolicies[SamProjectPolicy](authHeader, SamResourceType.Project)
      owningProjects = projectPolicies.collect {
        case x if (x.accessPolicyName == AccessPolicyName.Owner) => x.googleProject
      }
      runtimePolicies <- samDao
        .getResourcePolicies[SamRuntimePolicy](authHeader, SamResourceType.Runtime)
      createdPolicies = runtimePolicies.filter(_.accessPolicyName == AccessPolicyName.Creator)
    } yield runtimes.filter {
      case (project, samResource) =>
        owningProjects.contains(project) || createdPolicies.exists(_.resource == samResource)
    }
  }

  override def filterUserVisiblePersistentDisks(userInfo: UserInfo,
                                                disks: List[(GoogleProject, PersistentDiskSamResource)])(
    implicit ev: ApplicativeAsk[F, TraceId]
  ): F[List[(GoogleProject, PersistentDiskSamResource)]] = {
    val authHeader = Authorization(Credentials.Token(AuthScheme.Bearer, userInfo.accessToken.token))
    for {
      projectPolicies <- samDao.getResourcePolicies[SamProjectPolicy](authHeader, SamResourceType.Project)
      owningProjects = projectPolicies.collect {
        case x if (x.accessPolicyName == AccessPolicyName.Owner) => x.googleProject
      }
      diskPolicies <- samDao
        .getResourcePolicies[SamPersistentDiskPolicy](authHeader, SamResourceType.PersistentDisk)
      createdPolicies = diskPolicies.filter(_.accessPolicyName == AccessPolicyName.Creator)
    } yield disks.filter {
      case (project, samResource) =>
        owningProjects.contains(project) || createdPolicies.exists(_.resource == samResource)
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

final case class SamAuthProviderConfig(notebookAuthCacheEnabled: Boolean,
                                       notebookAuthCacheMaxSize: Int = 1000,
                                       notebookAuthCacheExpiryTime: FiniteDuration = 15 minutes)
private[sam] case class NotebookAuthCacheKey(samResource: RuntimeSamResource,
                                             authorization: Authorization,
                                             action: RuntimeAction,
                                             googleProject: GoogleProject)
