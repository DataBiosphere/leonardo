package org.broadinstitute.dsde.workbench.leonardo.auth.sam

import java.util.concurrent.TimeUnit

import com.typesafe.config.Config
import akka.http.scaladsl.model.StatusCodes
import com.google.common.cache.{CacheBuilder, CacheLoader}
import io.swagger.client.ApiException
import net.ceedubs.ficus.Ficus._
import org.broadinstitute.dsde.workbench.leonardo.auth.sam.SamAuthProvider.{NotebookAuthCacheKey, SamCacheKey}
import org.broadinstitute.dsde.workbench.leonardo.model._
import org.broadinstitute.dsde.workbench.leonardo.model.google.ClusterName
import org.broadinstitute.dsde.workbench.leonardo.model.NotebookClusterActions._
import org.broadinstitute.dsde.workbench.leonardo.model.ProjectActions.CreateClusters
import org.broadinstitute.dsde.workbench.model.{UserInfo, WorkbenchEmail}
import org.broadinstitute.dsde.workbench.model.google.GoogleProject

import scala.concurrent.duration._
import scala.concurrent._

case class UnknownLeoAuthAction(action: LeoAuthAction)
  extends LeoException(s"SamAuthProvider has no mapping for authorization action ${action.toString}, and is therefore probably out of date.", StatusCodes.InternalServerError)

object SamAuthProvider {
  private[sam] sealed trait SamCacheKey
  private[sam] case class NotebookAuthCacheKey(userInfo: UserInfo, action: NotebookClusterAction, googleProject: GoogleProject, clusterName: ClusterName, executionContext: ExecutionContext) extends SamCacheKey
}

class SamAuthProvider(val config: Config, serviceAccountProvider: ServiceAccountProvider) extends LeoAuthProvider(config, serviceAccountProvider) with SamProvider {

  // This causes Leo to try Sam calls up to 3 times, sleeping 500 milliseconds between each try
  private val samRetryInterval = 500 milliseconds
  private val samRetryTimeout = 1 second

  private lazy val notebookAuthCacheEnabled = config.getOrElse("notebookAuthCacheEnabled", true)
  private lazy val notebookAuthCacheMaxSize = config.getAs[Int]("notebookAuthCacheMaxSize").getOrElse(1000)
  private lazy val notebookAuthCacheExpiryTime = config.getAs[FiniteDuration]("notebookAuthCacheExpiryTime").getOrElse(15 minutes)

  // Cache notebook auth results from Sam as this is called very often by the proxy and the "list clusters" endpoint.
  // Project-level auth is not cached because it's just called for "create cluster" which is not as frequent.
  private[sam] val notebookAuthCache = CacheBuilder.newBuilder()
    .expireAfterWrite(notebookAuthCacheExpiryTime.toSeconds, TimeUnit.SECONDS)
    .maximumSize(notebookAuthCacheMaxSize)
    .build(
      new CacheLoader[SamCacheKey, Future[Boolean]] {
        def load(key: SamCacheKey) = {
          key match {
            case NotebookAuthCacheKey(userInfo, action, googleProject, clusterName, executionContext) =>
              // tokenExpiresIn should not taken into account when comparing cache keys
              hasNotebookClusterPermissionInternal(userInfo.copy(tokenExpiresIn = 0), action, googleProject, clusterName)(executionContext)
          }
        }
      }
    )

  protected def getProjectActionString(action: LeoAuthAction): String = {
    projectActionMap.getOrElse(action, throw UnknownLeoAuthAction(action))
  }

  protected def getNotebookActionString(action: LeoAuthAction): String = {
    notebookActionMap.getOrElse(action, throw UnknownLeoAuthAction(action))
  }

  val projectActionMap: Map[LeoAuthAction, String] = Map(
    GetClusterStatus -> "list_notebook_cluster",
    CreateClusters -> "launch_notebook_cluster",
    SyncDataToCluster -> "sync_notebook_cluster",
    DeleteCluster -> "delete_notebook_cluster",
    StopStartCluster -> "stop_start_notebook_cluster")

  val notebookActionMap: Map[LeoAuthAction, String] = Map(
    GetClusterStatus -> "status",
    ConnectToCluster -> "connect",
    SyncDataToCluster -> "sync",
    DeleteCluster -> "delete",
    StopStartCluster -> "stop_start")

  /**
    * @param userInfo The user in question
    * @param action The project-level action (above) the user is requesting
    * @param googleProject The Google project to check in
    * @return If the given user has permissions in this project to perform the specified action.
    */
  override def hasProjectPermission(userInfo: UserInfo, action: ProjectActions.ProjectAction, googleProject: GoogleProject)(implicit executionContext: ExecutionContext): Future[Boolean] = {
    retryUntilSuccessOrTimeout(shouldInvalidateSamCacheAndRetry, s"SamAuthProvider.hasProjectPermission call failed for ${userInfo.userEmail.value} check in project ${googleProject.value}")(samRetryInterval, samRetryTimeout) { () =>
      Future {
        blocking(samClient.hasActionOnBillingProjectResource(userInfo, googleProject, getProjectActionString(action)))
      }
    }
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
  override def hasNotebookClusterPermission(userInfo: UserInfo, action: NotebookClusterActions.NotebookClusterAction, googleProject: GoogleProject, clusterName: ClusterName)(implicit executionContext: ExecutionContext): Future[Boolean] = {
    // Consult the notebook auth cache if enabled
    if (notebookAuthCacheEnabled) {
      // tokenExpiresIn should not taken into account when comparing cache keys
      notebookAuthCache.get(NotebookAuthCacheKey(userInfo.copy(tokenExpiresIn = 0), action, googleProject, clusterName, executionContext))
    } else {
      hasNotebookClusterPermissionInternal(userInfo, action, googleProject, clusterName)
    }
  }

  private def hasNotebookClusterPermissionInternal(userInfo: UserInfo, action: NotebookClusterActions.NotebookClusterAction, googleProject: GoogleProject, clusterName: ClusterName)(implicit executionContext: ExecutionContext): Future[Boolean] = {
    // if action is connect, check only cluster resource. If action is anything else, either cluster or project must be true
    retryUntilSuccessOrTimeout(shouldInvalidateSamCacheAndRetry, s"SamAuthProvider.hasNotebookClusterPermissionInternal call failed for ${googleProject.value}/${clusterName.value}")(samRetryInterval, samRetryTimeout) { () =>
      Future {
        val hasNotebookAction = blocking(samClient.hasActionOnNotebookClusterResource(userInfo, googleProject, clusterName, getNotebookActionString(action)))
        if (action == ConnectToCluster) {
          hasNotebookAction
        } else {
          hasNotebookAction || blocking(samClient.hasActionOnBillingProjectResource(userInfo, googleProject, getProjectActionString(action)))
        }
      }
    }
  }

  /**
    * Leo calls this method when it receives a "list clusters" API call, passing in all non-deleted clusters from the database.
    * It should return a list of clusters that the user can see according to their authz.
    *
    * @param userInfo The user in question
    * @param clusters All non-deleted clusters from the database
    * @return         Filtered list of clusters that the user is allowed to see
    */
  override def filterUserVisibleClusters(userInfo: UserInfo, clusters: List[(GoogleProject, ClusterName)])(implicit executionContext: ExecutionContext): Future[List[(GoogleProject, ClusterName)]] = {
    for {
      owningProjects <- retryableFutureToFuture(retryUntilSuccessOrTimeout(shouldInvalidateSamCacheAndRetry, s"SamAuthProvider.filterUserVisibleClusters.owningProjects call failed")(samRetryInterval, samRetryTimeout) { () =>
        Future(blocking(samClient.listOwningProjects(userInfo)).toSet)
      })
      createdClusters <- retryableFutureToFuture(retryUntilSuccessOrTimeout(shouldInvalidateSamCacheAndRetry, s"SamAuthProvider.filterUserVisibleClusters.createdClusters call failed")(samRetryInterval, samRetryTimeout) { () =>
        Future(blocking(samClient.listCreatedClusters(userInfo)).toSet)
      })
    } yield {
      clusters.filter { case (project, name) =>
        owningProjects.contains(project) || createdClusters.contains((project, name))
      }
    }
  }

  //Notifications that Leo has created/destroyed clusters. Allows the auth provider to register things.

  /**
    * Leo calls this method to notify the auth provider that a new notebook cluster has been created.
    * The returned future should complete once the provider has finished doing any associated work.
    * Returning a failed Future will prevent the cluster from being created, and will call notifyClusterDeleted for the same cluster.
    * Leo will wait, so be timely!
    *
    * @param creatorEmail     The email address of the user in question
    * @param googleProject The Google project the cluster was created in
    * @param clusterName   The user-provided name of the Dataproc cluster
    * @return A Future that will complete when the auth provider has finished doing its business.
    */
  override def notifyClusterCreated(creatorEmail: WorkbenchEmail, googleProject: GoogleProject, clusterName: ClusterName)(implicit executionContext: ExecutionContext): Future[Unit] = {
    retryUntilSuccessOrTimeout(shouldInvalidateSamCacheAndRetry, s"SamAuthProvider.notifyClusterCreated call failed for ${googleProject.value}/${clusterName.value}")(samRetryInterval, samRetryTimeout) {  () =>
      Future {
        blocking(samClient.createNotebookClusterResource(creatorEmail, googleProject, clusterName))
      }.recover {
        case e if shouldInvalidateSamCacheAndRetry(e) =>
          // invalidate the pet token cache between retries in case it contains stale entries
          // See https://github.com/DataBiosphere/leonardo/issues/290
          samClient.invalidatePetAccessToken(creatorEmail, googleProject)
          throw e
      }
    }
  }

  /**
    * Leo calls this method to notify the auth provider that a notebook cluster has been deleted.
    * The returned future should complete once the provider has finished doing any associated work.
    * Leo will wait, so be timely!
    *
    * @param userEmail        The email address of the user in question
    * @param creatorEmail     The email address of the creator of the cluster
    * @param googleProject    The Google project the cluster was created in
    * @param clusterName      The user-provided name of the Dataproc cluster
    * @return A Future that will complete when the auth provider has finished doing its business.
    */
  override def notifyClusterDeleted(userEmail: WorkbenchEmail, creatorEmail: WorkbenchEmail, googleProject: GoogleProject, clusterName: ClusterName)
                                   (implicit executionContext: ExecutionContext): Future[Unit] = {
    retryUntilSuccessOrTimeout(shouldInvalidateSamCacheAndRetry, s"SamAuthProvider.notifyClusterDeleted call failed for ${googleProject.value}/${clusterName.value}")(samRetryInterval, samRetryTimeout) { () =>
      Future {
        blocking(samClient.deleteNotebookClusterResource(creatorEmail, googleProject, clusterName))
      }.recover {
        // treat 404s from Sam as the cluster already being deleted
        case e: ApiException if e.getCode == StatusCodes.NotFound.intValue => ()
        case e if shouldInvalidateSamCacheAndRetry(e) =>
          // invalidate the pet token cache between retries in case it contains stale entries
          // See https://github.com/DataBiosphere/leonardo/issues/290
          samClient.invalidatePetAccessToken(creatorEmail, googleProject)
          throw e
      }
    }
  }

  private def shouldInvalidateSamCacheAndRetry(t: Throwable): Boolean = {
    t match {
      // invalidate and retry 401s because the pet token cached in SwaggerSamClient might be stale
      case e: ApiException if e.getCode == 401 => true
      // always invalidate and retry 500 errors
      case e: ApiException if e.getCode / 100 == 5 => true
      // otherwise don't retry
      case _ => false
    }
  }

}
