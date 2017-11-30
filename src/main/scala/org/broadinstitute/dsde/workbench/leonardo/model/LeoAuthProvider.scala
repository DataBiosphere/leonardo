package org.broadinstitute.dsde.workbench.leonardo.model

import java.util.UUID

import com.typesafe.config.Config

import scala.concurrent.Future

object ProjectActions {
  sealed trait ProjectAction extends Product with Serializable
  case object ListClusters extends ProjectAction
  case object CreateClusters extends ProjectAction
  val allActions = Seq(ListClusters, CreateClusters)
}

object NotebookClusterActions {
  sealed trait NotebookClusterAction extends Product with Serializable
  case object GetClusterDetails extends NotebookClusterAction
  case object ConnectToCluster extends NotebookClusterAction
  case object LocalizeDataToCluster extends NotebookClusterAction
  case object DestroyCluster extends NotebookClusterAction
  val allActions = Seq(GetClusterDetails, ConnectToCluster, LocalizeDataToCluster, DestroyCluster)

}

abstract class LeoAuthProvider(authConfig: Config) {
  /**
    * @param userEmail The email address of the user in question
    * @param action The project-level action (above) the user is requesting
    * @param googleProject The Google project to check in
    * @return If the given user has permissions in this project to perform the specified action.
    */
  def hasProjectPermission(userEmail: String, action: ProjectActions.ProjectAction, googleProject: String): Future[Boolean]

  /**
    * @param userEmail The email address of the user in question
    * @param action The cluster-level action (above) the user is requesting
    * @param clusterGoogleID The UUID of the Dataproc cluster
    * @return If the userEmail has permission on this individual notebook cluster to perform this action
    */
  def hasNotebookClusterPermission(userEmail: String, action: NotebookClusterActions.NotebookClusterAction, clusterGoogleID: UUID): Future[Boolean]

  //Notifications that Leo has created/destroyed clusters. Allows the auth provider to register things.

  /**
    * Leo calls this method to notify the auth provider that a new notebook cluster has been created.
    * The returned future should complete once the provider has finished doing any associated work.
    * Leo will wait, so be timely!
    *
    * @param userEmail The email address of the user in question
    * @param googleProject The Google project the cluster was created in
    * @param clusterGoogleID The unique ID of the Dataproc cluster
    * @return A Future that will complete when the auth provider has finished doing its business.
    */
  def notifyClusterCreated(userEmail: String, googleProject: String, clusterGoogleID: UUID): Future[Unit]

  /**
    * Leo calls this method to notify the auth provider that a notebook cluster has been destroyed.
    * The returned future should complete once the provider has finished doing any associated work.
    * Leo will wait, so be timely!
    *
    * @param userEmail The email address of the user in question
    * @param googleProject The Google project the cluster was created in
    * @param clusterGoogleID The unique ID of the Dataproc cluster
    * @return A Future that will complete when the auth provider has finished doing its business.
    */
  def notifyClusterDestroyed(userEmail: String, googleProject: String, clusterGoogleID: UUID): Future[Unit]
}
