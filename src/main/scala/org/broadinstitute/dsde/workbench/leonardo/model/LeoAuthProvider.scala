package org.broadinstitute.dsde.workbench.leonardo.model

import com.typesafe.config.Config
import org.broadinstitute.dsde.workbench.model.WorkbenchEmail
import org.broadinstitute.dsde.workbench.model.google.GoogleProject

import scala.concurrent.{ExecutionContext, Future}


sealed trait LeoAuthAction extends Product with Serializable

object ProjectActions {
  sealed trait ProjectAction extends LeoAuthAction
  case object CreateClusters extends ProjectAction
  val allActions = Seq(CreateClusters)
}

object NotebookClusterActions {
  sealed trait NotebookClusterAction extends LeoAuthAction
  case object GetClusterStatus extends NotebookClusterAction
  case object ConnectToCluster extends NotebookClusterAction
  case object SyncDataToCluster extends NotebookClusterAction
  case object DeleteCluster extends NotebookClusterAction
  val allActions = Seq(GetClusterStatus, ConnectToCluster, SyncDataToCluster, DeleteCluster)

}

abstract class LeoAuthProvider(authConfig: Config, serviceAccountProvider: ServiceAccountProvider) {
  /**
    * @param userEmail The user in question
    * @param action The project-level action (above) the user is requesting
    * @param googleProject The Google project to check in
    * @return If the given user has permissions in this project to perform the specified action.
    */
  def hasProjectPermission(userEmail: WorkbenchEmail, action: ProjectActions.ProjectAction, googleProject: GoogleProject)(implicit executionContext: ExecutionContext): Future[Boolean]

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
  def canSeeAllClustersInProject(userEmail: WorkbenchEmail, googleProject: GoogleProject)(implicit executionContext: ExecutionContext): Future[Boolean] = {
    Future.successful(false)
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
  def hasNotebookClusterPermission(userEmail: WorkbenchEmail, action: NotebookClusterActions.NotebookClusterAction, googleProject: GoogleProject, clusterName: ClusterName)(implicit executionContext: ExecutionContext): Future[Boolean]

  //Notifications that Leo has created/destroyed clusters. Allows the auth provider to register things.

  /**
    * Leo calls this method to notify the auth provider that a new notebook cluster has been created.
    * The returned future should complete once the provider has finished doing any associated work.
    * Returning a failed Future will prevent the cluster from being created, and will call notifyClusterDeleted for the same cluster.
    * Leo will wait, so be timely!
    *
    * @param cluster        The Dataproc cluster
    * @return A Future that will complete when the auth provider has finished doing its business.
    */
  def notifyClusterCreated(cluster: Cluster)(implicit executionContext: ExecutionContext): Future[Unit]

  /**
    * Leo calls this method to notify the auth provider that a notebook cluster has been deleted.
    * The returned future should complete once the provider has finished doing any associated work.
    * Leo will wait, so be timely!
    *
    * @param cluster        The Dataproc cluster
    * @return A Future that will complete when the auth provider has finished doing its business.
    */
  def notifyClusterDeleted(cluster: Cluster)(implicit executionContext: ExecutionContext): Future[Unit]
}
