package org.broadinstitute.dsde.workbench.leonardo.model

import java.util.UUID

import com.typesafe.config.Config

import scala.concurrent.Future

sealed trait ProjectAction
case object ListClusters extends ProjectAction
case object CreateClusters extends ProjectAction

sealed trait NotebookClusterAction
case object GetClusterDetails extends NotebookClusterAction
case object ConnectToCluster extends NotebookClusterAction
case object LocalizeDataToCluster extends NotebookClusterAction
case object DestroyCluster extends NotebookClusterAction

abstract class LeoAuthProvider(authConfig: Config) {
  /**
    * @param userEmail The email address of the user in question
    * @param action The project-level action (above) the user is requesting
    * @param project The Google project to check in
    * @return If the given user has permissions in this project to perform the specified action.
    */
  def hasProjectPermission(userEmail: String, action: ProjectAction, project: GoogleProject): Future[Boolean]

  /**
    * @param userEmail The email address of the user in question
    * @param action The cluster-level action (above) the user is requesting
    * @param clusterGoogleID The UUID of the Dataproc cluster
    * @return If the userEmail has permission on this individual notebook cluster to perform this action
    */
  def hasNotebookClusterPermission(userEmail: String, action: NotebookClusterAction, clusterGoogleID: UUID): Future[Boolean]

  //Notifications that Leo has created/destroyed clusters. Allows the auth provider to register things.

  /**
    * Leo calls this method to notify the auth provider that a new notebook cluster has been created.
    * The returned future should complete once the provider has finished doing any associated work.
    * Leo will wait, so be timely!
    *
    * @param userEmail The email address of the user in question
    * @param project The Google project the cluster was created in
    * @param clusterGoogleID The unique ID of the Dataproc cluster
    * @return A Future that will complete when the auth provider has finished doing its business.
    */
  def notifyClusterCreated(userEmail: String, project: GoogleProject, clusterGoogleID: UUID): Future[Unit]

  /**
    * Leo calls this method to notify the auth provider that a notebook cluster has been destroyed.
    * The returned future should complete once the provider has finished doing any associated work.
    * Leo will wait, so be timely!
    *
    * @param userEmail The email address of the user in question
    * @param project The Google project the cluster was created in
    * @param clusterGoogleID The unique ID of the Dataproc cluster
    * @return A Future that will complete when the auth provider has finished doing its business.
    */
  def notifyClusterDestroyed(userEmail: String, project: GoogleProject, clusterGoogleID: UUID): Future[Unit]
}
