package org.broadinstitute.dsde.workbench.leonardo.model

import java.util.UUID

import org.broadinstitute.dsde.workbench.leonardo.api.AuthorizationError
import org.broadinstitute.dsde.workbench.model.WorkbenchUserEmail

import scala.concurrent.Future

sealed trait ProjectAction
case object ListClusters extends ProjectAction
case object CreateClusters extends ProjectAction

sealed trait NotebookClusterAction
case object GetClusterDetails extends NotebookClusterAction
case object ConnectToCluster extends NotebookClusterAction
case object LocalizeDataToCluster extends NotebookClusterAction
case object DestroyCluster extends NotebookClusterAction

abstract class LeoAuthProvider {
  //Does this user have permission in all projects to perform this action?
  def hasPermissionInAllProjects(user: WorkbenchUserEmail, action: ProjectAction): Future[Boolean]

  //Does the user have permission in this project to perform this action?
  def hasProjectPermission(user: WorkbenchUserEmail, action: ProjectAction, project: GoogleProject): Future[Boolean]

  //List the projects in which the user has ListClusters action.
  //If hasPermissionInAllProjects(_, ListClusters) returns true in your implementation, you should return Seq.empty() for this function.
  def getProjectsWithListClustersPermission(user: WorkbenchUserEmail): Future[Set[GoogleProject]]

  //Does the user have permission on this individual notebook cluster to perform this action?
  def hasNotebookClusterPermission(user: WorkbenchUserEmail, action: NotebookClusterAction, clusterGoogleID: UUID): Future[Boolean]

  //Notifications that Leo has created/destroyed clusters.
  //The resulting future should return once the provider has finished doing any associated work. Leo will wait.
  def notifyClusterCreated(user: WorkbenchUserEmail, project: GoogleProject, clusterGoogleID: UUID): Future[Unit]
  def notifyClusterDestroyed(user: WorkbenchUserEmail, project: GoogleProject, clusterGoogleID: UUID): Future[Unit]
}
