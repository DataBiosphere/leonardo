package org.broadinstitute.dsde.workbench.leonardo.model

import java.io.File

import com.typesafe.config.Config
import org.broadinstitute.dsde.workbench.leonardo.model.google.ClusterName
import org.broadinstitute.dsde.workbench.model.{UserInfo, WorkbenchEmail}
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
  case object ModifyCluster extends NotebookClusterAction
  case object StopStartCluster extends NotebookClusterAction
  val allActions = Seq(GetClusterStatus, ConnectToCluster, SyncDataToCluster, DeleteCluster, StopStartCluster)

}

abstract class LeoAuthProvider(authConfig: Config, serviceAccountProvider: ServiceAccountProvider) {

  /**
    * @param userInfo The user in question
    * @param action The project-level action (above) the user is requesting
    * @param googleProject The Google project to check in
    * @return If the given user has permissions in this project to perform the specified action.
    */
  def hasProjectPermission(userInfo: UserInfo, action: ProjectActions.ProjectAction, googleProject: GoogleProject)(implicit executionContext: ExecutionContext): Future[Boolean]

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
  def hasNotebookClusterPermission(internalId: String, userInfo: UserInfo, action: NotebookClusterActions.NotebookClusterAction, googleProject: GoogleProject, clusterName: ClusterName)(implicit executionContext: ExecutionContext): Future[Boolean]

  /**
    * Leo calls this method when it receives a "list clusters" API call, passing in all non-deleted clusters from the database.
    * It should return a list of clusters that the user can see according to their authz.
    *
    * @param userInfo The user in question
    * @param clusters All non-deleted clusters from the database
    * @return         Filtered list of clusters that the user is allowed to see
    */
  def filterUserVisibleClusters(userInfo: UserInfo, clusters: List[(GoogleProject, ClusterName)])(implicit executionContext: ExecutionContext): Future[List[(GoogleProject, ClusterName)]]

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
  def notifyClusterCreated(internalId: String, creatorEmail: WorkbenchEmail, googleProject: GoogleProject, clusterName: ClusterName)(implicit executionContext: ExecutionContext): Future[Unit]

  /**
    * Leo calls this method to notify the auth provider that a notebook cluster has been deleted.
    * The returned future should complete once the provider has finished doing any associated work.
    * Leo will wait, so be timely!
    *
    * @param internalId     The internal ID for the cluster (i.e. used for Sam resources)
    * @param userEmail     The email address of the user in question
    * @param creatorEmail  The email address of the creator of the cluster
    * @param googleProject The Google project the cluster was created in
    * @param clusterName   The user-provided name of the Dataproc cluster
    * @return A Future that will complete when the auth provider has finished doing its business.
    */
  def notifyClusterDeleted(internalId: String, userEmail: WorkbenchEmail, creatorEmail: WorkbenchEmail, googleProject: GoogleProject, clusterName: ClusterName)(implicit executionContext: ExecutionContext): Future[Unit]

  protected def getLeoServiceAccountAndKey: (WorkbenchEmail, File) = {
    serviceAccountProvider.getLeoServiceAccountAndKey
  }
}
