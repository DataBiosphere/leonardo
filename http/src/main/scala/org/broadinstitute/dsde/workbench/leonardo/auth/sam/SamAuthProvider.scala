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
import org.broadinstitute.dsde.workbench.leonardo.dao.HttpSamDAO._
import org.broadinstitute.dsde.workbench.leonardo.dao.{ResourceTypeName, SamDAO, _}
import org.broadinstitute.dsde.workbench.leonardo.model.NotebookClusterActions._
import org.broadinstitute.dsde.workbench.leonardo.model.PersistentDiskActions._
import org.broadinstitute.dsde.workbench.leonardo.model.ProjectActions.{CreateClusters, CreatePersistentDisk}
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

  // Cache notebook auth results from Sam as this is called very often by the proxy and the "list clusters" endpoint.
  // Project-level auth is not cached because it's just called for "create cluster" which is not as frequent.
  private[sam] val notebookAuthCache = CacheBuilder
    .newBuilder()
    .expireAfterWrite(config.notebookAuthCacheExpiryTime.toSeconds, TimeUnit.SECONDS)
    .maximumSize(config.notebookAuthCacheMaxSize)
    .build(
      new CacheLoader[NotebookAuthCacheKey, java.lang.Boolean] {
        override def load(key: NotebookAuthCacheKey): java.lang.Boolean = {
          implicit val traceId = ApplicativeAsk.const[F, TraceId](TraceId(UUID.randomUUID()))
          checkNotebookClusterPermissionWithProjectFallback(key.internalId,
                                                            key.authorization,
                                                            key.action,
                                                            key.googleProject,
                                                            key.clusterName).toIO.unsafeRunSync()
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
    notebookActionMap
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
    GetClusterStatus -> "list_notebook_cluster",
    CreateClusters -> "launch_notebook_cluster",
    SyncDataToCluster -> "sync_notebook_cluster",
    DeleteCluster -> "delete_notebook_cluster",
    StopStartCluster -> "stop_start_notebook_cluster",
    CreatePersistentDisk -> "create_persistent_disk",
    DeletePersistentDisk -> "delete_persistent_disk"

  )

  private val notebookActionMap: Map[LeoAuthAction, String] = Map(
    GetClusterStatus -> "status",
    ConnectToCluster -> "connect",
    SyncDataToCluster -> "sync",
    DeleteCluster -> "delete",
    ModifyCluster -> "modify",
    StopStartCluster -> "stop_start"
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

  override def hasNotebookClusterPermission(
    internalId: RuntimeInternalId,
    userInfo: UserInfo,
    action: NotebookClusterAction,
    googleProject: GoogleProject,
    clusterName: RuntimeName
  )(implicit ev: ApplicativeAsk[F, TraceId]): F[Boolean] = {
    val authorization = Authorization(Credentials.Token(AuthScheme.Bearer, userInfo.accessToken.token))
    // Consult the notebook auth cache if enabled
    if (config.notebookAuthCacheEnabled) {
      // tokenExpiresIn should not taken into account when comparing cache keys
      blocker.blockOn(
        Effect[F].delay(
          notebookAuthCache
            .get(NotebookAuthCacheKey(internalId, authorization, action, googleProject, clusterName))
            .booleanValue()
        )
      )
    } else {
      checkNotebookClusterPermissionWithProjectFallback(internalId, authorization, action, googleProject, clusterName)
    }
  }

  override def hasPersistentDiskPermission(internalId: PersistentDiskInternalId,
                                           userInfo: UserInfo,
                                           action: PersistentDiskAction,
                                           googleProject: GoogleProject
                                         )(implicit ev: ApplicativeAsk[F, TraceId]): F[Boolean] = {
    val authorization = Authorization(Credentials.Token(AuthScheme.Bearer, userInfo.accessToken.token))
    // can add check to cache here if necessary
    checkPersistentDiskPermissionWithProjectFallback(internalId, authorization, action, googleProject)
  }

  private def checkNotebookClusterPermissionWithProjectFallback(
    internalId: RuntimeInternalId,
    authorization: Authorization,
    action: NotebookClusterAction,
    googleProject: GoogleProject,
    clusterName: RuntimeName
  )(implicit ev: ApplicativeAsk[F, TraceId]): F[Boolean] =
    for {
      traceId <- ev.ask
      hasNotebookAction <- hasNotebookClusterPermissionInternal(internalId, action, authorization)
      res <- if (action == ConnectToCluster) Sync[F].pure(hasNotebookAction)
      else {
        if (hasNotebookAction)
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

  private def checkPersistentDiskPermissionWithProjectFallback(internalId: PersistentDiskInternalId,
                                                               authorization: Authorization,
                                                               action: PersistentDiskAction,
                                                               googleProject: GoogleProject
                                                              )(implicit ev: ApplicativeAsk[F, TraceId]): F[Boolean] =
    for {
      traceId <- ev.ask
      hasPersistentDiskAction <- hasPersistentDiskPermissionInternal(internalId, action, authorization)
      diskLevelActions = Set(ReadPersistentDisk, AttachPersistentDisk, ModifyPersistentDisk)
      res <- if (diskLevelActions contains action) Sync[F].pure(hasPersistentDiskAction)
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
      res <- samDao.hasResourcePermission(googleProject.value,
                                          actionString,
                                          ResourceTypeName.BillingProject,
                                          authHeader)
    } yield res

  private def hasNotebookClusterPermissionInternal(
    clusterInternalId: RuntimeInternalId,
    action: LeoAuthAction,
    authHeader: Authorization
  )(implicit ev: ApplicativeAsk[F, TraceId]): F[Boolean] =
    for {
      actionString <- Effect[F].fromEither(getNotebookActionString(action))
      res <- samDao.hasResourcePermission(clusterInternalId.asString,
                                          actionString,
                                          ResourceTypeName.NotebookCluster,
                                          authHeader)
    } yield res

  private def hasPersistentDiskPermissionInternal(persistentDiskInternalId: PersistentDiskInternalId,
                                                  action: LeoAuthAction,
                                                  authHeader: Authorization
                                                )(implicit ev: ApplicativeAsk[F, TraceId]): F[Boolean] =
    for {
      actionString <- Effect[F].fromEither(getPersistentDiskActionString(action))
      res <- samDao.hasResourcePermission(persistentDiskInternalId.asString,
        actionString,
        ResourceTypeName.PersistentDisk,
        authHeader)
    } yield res

  override def filterUserVisibleClusters(userInfo: UserInfo, clusters: List[(GoogleProject, RuntimeInternalId)])(
    implicit ev: ApplicativeAsk[F, TraceId]
  ): F[List[(GoogleProject, RuntimeInternalId)]] = {
    val authHeader = Authorization(Credentials.Token(AuthScheme.Bearer, userInfo.accessToken.token))
    for {
      projectPolicies <- samDao.getResourcePolicies[SamProjectPolicy](authHeader, ResourceTypeName.BillingProject)
      owningProjects = projectPolicies.collect {
        case x if (x.accessPolicyName == AccessPolicyName.Owner) => x.googleProject
      }
      clusterPolicies <- samDao
        .getResourcePolicies[SamNotebookClusterPolicy](authHeader, ResourceTypeName.NotebookCluster)
      createdPolicies = clusterPolicies.filter(_.accessPolicyName == AccessPolicyName.Creator)
    } yield clusters.filter {
      case (project, internalId) =>
        owningProjects.contains(project) || createdPolicies.exists(_.internalId == internalId)
    }
  }

  override def filterUserVisiblePersistentDisks(userInfo: UserInfo, disks: List[(GoogleProject, PersistentDiskInternalId)])(
    implicit ev: ApplicativeAsk[F, TraceId]
  ): F[List[(GoogleProject, PersistentDiskInternalId)]] = {
    val authHeader = Authorization(Credentials.Token(AuthScheme.Bearer, userInfo.accessToken.token))
    for {
      projectPolicies <- samDao.getResourcePolicies[SamProjectPolicy](authHeader, ResourceTypeName.BillingProject)
      owningProjects = projectPolicies.collect {
        case x if (x.accessPolicyName == AccessPolicyName.Owner) => x.googleProject
      }
      diskPolicies <- samDao
        .getResourcePolicies[SamPersistentDiskPolicy](authHeader, ResourceTypeName.PersistentDisk)
      createdPolicies = diskPolicies.filter(_.accessPolicyName == AccessPolicyName.Creator)
    } yield disks.filter {
      case (project, internalId) =>
        owningProjects.contains(project) || createdPolicies.exists(_.internalId == internalId)
    }
  }

  override def notifyClusterCreated(internalId: RuntimeInternalId,
                                    creatorEmail: WorkbenchEmail,
                                    googleProject: GoogleProject,
                                    clusterName: RuntimeName)(implicit ev: ApplicativeAsk[F, TraceId]): F[Unit] =
    samDao.createClusterResource(internalId, creatorEmail, googleProject, clusterName)

  override def notifyClusterDeleted(internalId: RuntimeInternalId,
                                    userEmail: WorkbenchEmail,
                                    creatorEmail: WorkbenchEmail,
                                    googleProject: GoogleProject,
                                    clusterName: RuntimeName)(implicit ev: ApplicativeAsk[F, TraceId]): F[Unit] =
    samDao.deleteClusterResource(internalId, userEmail, creatorEmail, googleProject, clusterName)

  override def notifyPersistentDiskCreated(internalId: PersistentDiskInternalId,
                                           creatorEmail: WorkbenchEmail,
                                           googleProject: GoogleProject)(implicit ev: ApplicativeAsk[F, TraceId]): F[Unit] =
    samDao.createPersistentDiskResource(internalId, creatorEmail, googleProject)

  override def notifyPersistentDiskDeleted(internalId: PersistentDiskInternalId,
                                           userEmail: WorkbenchEmail,
                                           creatorEmail: WorkbenchEmail,
                                           googleProject: GoogleProject)(implicit ev: ApplicativeAsk[F, TraceId]): F[Unit] =
    samDao.deletePersistentDiskResource(internalId, userEmail, creatorEmail, googleProject)
}
final case class SamAuthProviderConfig(notebookAuthCacheEnabled: Boolean,
                                       notebookAuthCacheMaxSize: Int = 1000,
                                       notebookAuthCacheExpiryTime: FiniteDuration = 15 minutes)
private[sam] case class NotebookAuthCacheKey(internalId: RuntimeInternalId,
                                             authorization: Authorization,
                                             action: NotebookClusterAction,
                                             googleProject: GoogleProject,
                                             clusterName: RuntimeName)
