package org.broadinstitute.dsde.workbench.leonardo.auth

import java.util.UUID

import com.typesafe.config.Config
import org.broadinstitute.dsde.workbench.leonardo.model._

import scala.concurrent.Future

class MockLeoAuthProvider(authConfig: Config) extends LeoAuthProvider(authConfig) {
  //behaviour defined in test\...\reference.conf
  val projectPermissions: Map[ProjectActions.ProjectAction, Boolean] =
    (ProjectActions.allActions map (action => action -> authConfig.getBoolean(action.toString) )).toMap
  val clusterPermissions: Map[NotebookClusterActions.NotebookClusterAction, Boolean] =
    (NotebookClusterActions.allActions map (action => action -> authConfig.getBoolean(action.toString) )).toMap

  def hasProjectPermission(userEmail: String, action: ProjectActions.ProjectAction, googleProject: String): Future[Boolean] = {
    Future.successful(projectPermissions(action))
  }

  def hasNotebookClusterPermission(userEmail: String, action: NotebookClusterActions.NotebookClusterAction, clusterGoogleID: UUID): Future[Boolean] = {
    Future.successful(clusterPermissions(action))
  }

  def notifyClusterCreated(userEmail: String, googleProject: String, clusterGoogleID: UUID): Future[Unit] = Future.successful(())
  def notifyClusterDestroyed(userEmail: String, googleProject: String, clusterGoogleID: UUID): Future[Unit] = Future.successful(())
}
