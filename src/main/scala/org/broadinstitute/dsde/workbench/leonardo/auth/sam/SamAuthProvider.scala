package org.broadinstitute.dsde.workbench.leonardo.auth.sam

import java.util.concurrent.TimeUnit

import com.typesafe.config.Config
import com.typesafe.scalalogging.LazyLogging
import akka.http.scaladsl.model.StatusCodes
import com.google.common.cache.{CacheBuilder, CacheLoader}
import net.ceedubs.ficus.Ficus._
import org.broadinstitute.dsde.workbench.leonardo.auth.sam.SamAuthProvider.NotebookAuthCacheKey
import org.broadinstitute.dsde.workbench.leonardo.model._
import org.broadinstitute.dsde.workbench.leonardo.model.google.ClusterName
import org.broadinstitute.dsde.workbench.leonardo.model.NotebookClusterActions._
import org.broadinstitute.dsde.workbench.leonardo.model.ProjectActions.CreateClusters
import org.broadinstitute.dsde.workbench.model.WorkbenchEmail
import org.broadinstitute.dsde.workbench.model.google.GoogleProject

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}

case class UnknownLeoAuthAction(action: LeoAuthAction)
  extends LeoException(s"SamAuthProvider has no mapping for authorization action ${action.toString}, and is therefore probably out of date.", StatusCodes.InternalServerError)

object SamAuthProvider {
  private[sam] case class NotebookAuthCacheKey(userEmail: WorkbenchEmail, action: NotebookClusterAction, googleProject: GoogleProject, clusterName: ClusterName, executionContext: ExecutionContext)
}

class SamAuthProvider(val config: Config, serviceAccountProvider: ServiceAccountProvider) extends LeoAuthProvider(config, serviceAccountProvider) with SamProvider with LazyLogging {

  private lazy val notebookAuthCacheMaxSize = config.getAs[Int]("notebookAuthCacheMaxSize").getOrElse(1000)
  private lazy val notebookAuthCacheExpiryTime = config.getAs[FiniteDuration]("notebookAuthCacheExpiryTime").getOrElse(15 minutes)

  // Cache notebook auth results from Sam as this is called very often by the proxy and the "list clusters" endpoint.
  // Project-level auth is not called as frequently so it's not as important to cache it.
  private[sam] val notebookAuthCache = CacheBuilder.newBuilder()
    .expireAfterWrite(notebookAuthCacheExpiryTime.toSeconds, TimeUnit.SECONDS)
    .maximumSize(notebookAuthCacheMaxSize)
    .build(
      new CacheLoader[NotebookAuthCacheKey, Future[Boolean]] {
        def load(key: NotebookAuthCacheKey) = {
          hasNotebookClusterPermissionInternal(key.userEmail, key.action, key.googleProject, key.clusterName)(key.executionContext)
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
    DeleteCluster -> "delete_notebook_cluster")


  val notebookActionMap: Map[LeoAuthAction, String] = Map(
    GetClusterStatus -> "status",
    ConnectToCluster -> "connect",
    SyncDataToCluster -> "sync",
    DeleteCluster -> "delete")

  /**
    * @param userEmail The user in question
    * @param action The project-level action (above) the user is requesting
    * @param googleProject The Google project to check in
    * @return If the given user has permissions in this project to perform the specified action.
    */
  override def hasProjectPermission(userEmail: WorkbenchEmail, action: ProjectActions.ProjectAction, googleProject: GoogleProject)(implicit executionContext: ExecutionContext): Future[Boolean] = {
    Future {
      samClient.hasActionOnBillingProjectResource(userEmail,googleProject, getProjectActionString(action))
    }
  }

  /**
    * When listing clusters, Leo will perform a GROUP BY on google projects and call this function once per google project.
    * If you have an implementation such that users, even in some cases, can see all clusters in a google project, overriding
    * this function may lead to significant performance improvements.
    * For any projects where this function call returns Future.successful(false), Leo will then call hasNotebookClusterPermission
    * for every cluster in that project, passing in action = GetClusterStatus.
    *
    * @param userEmail The user in question
    * @param googleProject A Google project
    * @return If the given user can see all clusters in this project
    */
  override def canSeeAllClustersInProject(userEmail: WorkbenchEmail, googleProject: GoogleProject)(implicit executionContext: ExecutionContext): Future[Boolean] = {
    Future {
      samClient.hasActionOnBillingProjectResource(userEmail,googleProject, "list_notebook_cluster")
    }
  }

  /**
    * Leo calls this method to verify if the user has permission to perform the given action on a specific notebook cluster.
    * It may call this method passing in a cluster that doesn't exist. Return Future.successful(false) if so.
    *
    * @param userEmail      The user in question
    * @param action        The cluster-level action (above) the user is requesting
    * @param googleProject The Google project the cluster was created in
    * @param clusterName   The user-provided name of the Dataproc cluster
    * @return If the userEmail has permission on this individual notebook cluster to perform this action
    */
  override def hasNotebookClusterPermission(userEmail: WorkbenchEmail, action: NotebookClusterActions.NotebookClusterAction, googleProject: GoogleProject, clusterName: ClusterName)(implicit executionContext: ExecutionContext): Future[Boolean] = {
    // Consult the notebook auth cache
    notebookAuthCache.get(NotebookAuthCacheKey(userEmail, action, googleProject, clusterName, executionContext))
  }

  private def hasNotebookClusterPermissionInternal(userEmail: WorkbenchEmail, action: NotebookClusterActions.NotebookClusterAction, googleProject: GoogleProject, clusterName: ClusterName)(implicit executionContext: ExecutionContext): Future[Boolean] = {
    // if action is connect, check only cluster resource. If action is anything else, either cluster or project must be true
    Future {
      val hasNotebookAction = samClient.hasActionOnNotebookClusterResource(userEmail,googleProject,clusterName, getNotebookActionString(action))
      if (action == ConnectToCluster) {
        hasNotebookAction
      } else {
        hasNotebookAction || samClient.hasActionOnBillingProjectResource(userEmail,googleProject, getProjectActionString(action))
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
    Future {
      samClient.createNotebookClusterResource(creatorEmail, googleProject, clusterName)
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
  override def notifyClusterDeleted(userEmail: WorkbenchEmail, creatorEmail: WorkbenchEmail, googleProject: GoogleProject, clusterName: ClusterName)(implicit executionContext: ExecutionContext): Future[Unit] = {
    Future {
      samClient.deleteNotebookClusterResource(creatorEmail, googleProject, clusterName)
    }
  }
}
