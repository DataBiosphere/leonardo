package org.broadinstitute.dsde.workbench.leonardo.model

import java.util.UUID

import org.broadinstitute.dsde.workbench.leonardo.api.AuthorizationError

import scala.concurrent.Future

sealed trait ProjectAction
case object ListClusters extends ProjectAction
case object CreateClusters extends ProjectAction

sealed trait NotebookClusterAction
case object GetClusterStatus extends NotebookClusterAction
case object ConnectToCluster extends NotebookClusterAction
case object LocalizeDataToCluster extends NotebookClusterAction
case object DestroyCluster extends NotebookClusterAction

abstract class LeoAuthProvider {
  def hasProjectPermission(user: UserInfo, action: ProjectAction, project: GoogleProject): Future[Boolean]
  def hasNotebookClusterPermission(user: UserInfo, action: NotebookClusterAction, clusterGoogleID: UUID): Future[Boolean]

  //notifications that Leo has created/destroyed clusters
  def notifyClusterCreated(user: UserInfo, project: GoogleProject, clusterGoogleID: UUID): Future[Unit]
  def notifyClusterDestroyed(user: UserInfo, project: GoogleProject, clusterGoogleID: UUID): Future[Unit]


}

class Blerg(authProvider: LeoAuthProvider) {

}
