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
import org.broadinstitute.dsde.workbench.leonardo.model.ProjectActions.CreateClusters
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

  private val projectActionMap: Map[LeoAuthAction, String] = Map(
    GetClusterStatus -> "list_notebook_cluster",
    CreateClusters -> "launch_notebook_cluster",
    SyncDataToCluster -> "sync_notebook_cluster",
    DeleteCluster -> "delete_notebook_cluster",
    StopStartCluster -> "stop_start_notebook_cluster"
  )

  private val notebookActionMap: Map[LeoAuthAction, String] = Map(
    GetClusterStatus -> "status",
    ConnectToCluster -> "connect",
    SyncDataToCluster -> "sync",
    DeleteCluster -> "delete",
    ModifyCluster -> "modify",
    StopStartCluster -> "stop_start"
  )

  /**
   * @param userInfo The user in question
   * @param action The project-level action (above) the user is requesting
   * @param googleProject The Google project to check in
   * @return If the given user has permissions in this project to perform the specified action.
   */
  override def hasProjectPermission(
    userInfo: UserInfo,
    action: ProjectActions.ProjectAction,
    googleProject: GoogleProject
  )(implicit ev: ApplicativeAsk[F, TraceId]): F[Boolean] = {
    val authHeader = Authorization(Credentials.Token(AuthScheme.Bearer, userInfo.accessToken.token))
    hasProjectPermissionInternal(googleProject, action, authHeader)
  }

  /**
   * Leo calls this method to verify if the user has permission to perform the given action on a specific notebook cluster.
   * It may call this method passing in a cluster that doesn't exist. Return Future.successful(false) if so.
   *
   * @param userInfo      The user in question
   * @param action        The cluster-level action (above) the user is requesting
   * @param googleProject The Google project the cluster was created in
   * @param clusterName   The user-provided name of the Dataproc cluster
   * @return If the userEmail has permission on this individual notebook cluster to perform this action
   */
  override def hasNotebookClusterPermission(
    internalId: ClusterInternalId,
    userInfo: UserInfo,
    action: NotebookClusterActions.NotebookClusterAction,
    googleProject: GoogleProject,
    clusterName: ClusterName
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

  private def checkNotebookClusterPermissionWithProjectFallback(
    internalId: ClusterInternalId,
    authorization: Authorization,
    action: NotebookClusterActions.NotebookClusterAction,
    googleProject: GoogleProject,
    clusterName: ClusterName
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
    clusterInternalId: ClusterInternalId,
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

  /**
   * Leo calls this method when it receives a "list clusters" API call, passing in all non-deleted clusters from the database.
   * It should return a list of clusters that the user can see according to their authz.
   *
   * @param userInfo The user in question
   * @param clusters All non-deleted clusters from the database
   * @return         Filtered list of clusters that the user is allowed to see
   */
  override def filterUserVisibleClusters(userInfo: UserInfo, clusters: List[(GoogleProject, ClusterInternalId)])(
    implicit ev: ApplicativeAsk[F, TraceId]
  ): F[List[(GoogleProject, ClusterInternalId)]] = {
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

  //Notifications that Leo has created/destroyed clusters. Allows the auth provider to register things.
  /**
   * Leo calls this method to notify the auth provider that a new notebook cluster has been created.
   * The returned future should complete once the provider has finished doing any associated work.
   * Returning a failed Future will prevent the cluster from being created, and will call notifyClusterDeleted for the same cluster.
   * Leo will wait, so be timely!
   *
   * @param internalId     The internal ID for the cluster (i.e. used for Sam resources)
   * @param creatorEmail     The email address of the user in question
   * @param googleProject The Google project the cluster was created in
   * @param clusterName   The user-provided name of the Dataproc cluster
   * @return A Future that will complete when the auth provider has finished doing its business.
   */
  override def notifyClusterCreated(internalId: ClusterInternalId,
                                    creatorEmail: WorkbenchEmail,
                                    googleProject: GoogleProject,
                                    clusterName: ClusterName)(implicit ev: ApplicativeAsk[F, TraceId]): F[Unit] =
    samDao.createClusterResource(internalId, creatorEmail, googleProject, clusterName)

  /**
   * Leo calls this method to notify the auth provider that a notebook cluster has been deleted.
   * The returned future should complete once the provider has finished doing any associated work.
   * Leo will wait, so be timely!
   *
   * @param internalId     The internal ID for the cluster (i.e. used for Sam resources)
   * @param userEmail        The email address of the user in question
   * @param creatorEmail     The email address of the creator of the cluster
   * @param googleProject    The Google project the cluster was created in
   * @param clusterName      The user-provided name of the Dataproc cluster
   * @return A Future that will complete when the auth provider has finished doing its business.
   */
  override def notifyClusterDeleted(internalId: ClusterInternalId,
                                    userEmail: WorkbenchEmail,
                                    creatorEmail: WorkbenchEmail,
                                    googleProject: GoogleProject,
                                    clusterName: ClusterName)(implicit ev: ApplicativeAsk[F, TraceId]): F[Unit] =
    samDao.deleteClusterResource(internalId, userEmail, creatorEmail, googleProject, clusterName)
}
final case class SamAuthProviderConfig(notebookAuthCacheEnabled: Boolean,
                                       notebookAuthCacheMaxSize: Int = 1000,
                                       notebookAuthCacheExpiryTime: FiniteDuration = 15 minutes)
private[sam] case class NotebookAuthCacheKey(internalId: ClusterInternalId,
                                             authorization: Authorization,
                                             action: NotebookClusterAction,
                                             googleProject: GoogleProject,
                                             clusterName: ClusterName)
