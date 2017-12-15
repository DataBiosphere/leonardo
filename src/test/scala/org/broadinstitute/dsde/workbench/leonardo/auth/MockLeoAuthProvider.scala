package org.broadinstitute.dsde.workbench.leonardo.auth

import java.util.UUID

import com.typesafe.config.Config
import org.broadinstitute.dsde.workbench.leonardo.model._
import org.broadinstitute.dsde.workbench.model.UserInfo

import scala.concurrent.{ExecutionContext, Future}

class MockLeoAuthProvider(authConfig: Config, notifySucceeds: Boolean = true) extends LeoAuthProvider(authConfig) {
  //behaviour defined in test\...\reference.conf
  val projectPermissions: Map[ProjectActions.ProjectAction, Boolean] =
    (ProjectActions.allActions map (action => action -> authConfig.getBoolean(action.toString) )).toMap
  val clusterPermissions: Map[NotebookClusterActions.NotebookClusterAction, Boolean] =
    (NotebookClusterActions.allActions map (action => action -> authConfig.getBoolean(action.toString) )).toMap

  def hasProjectPermission(userInfo: UserInfo, action: ProjectActions.ProjectAction, googleProject: String)(implicit executionContext: ExecutionContext): Future[Boolean] = {
    Future.successful(projectPermissions(action))
  }

  def hasNotebookClusterPermission(userInfo: UserInfo, action: NotebookClusterActions.NotebookClusterAction, googleProject: String, clusterName: String)(implicit executionContext: ExecutionContext): Future[Boolean] = {
    Future.successful(clusterPermissions(action))
  }

  private def notifyInternal = {
    if( notifySucceeds )
      Future.successful(())
    else
      Future.failed(new RuntimeException("boom"))
  }

  def notifyClusterCreated(userEmail: String, googleProject: String, clusterName: String)(implicit executionContext: ExecutionContext): Future[Unit] = notifyInternal

  def notifyClusterDeleted(userEmail: String, googleProject: String, clusterName: String)(implicit executionContext: ExecutionContext): Future[Unit] = notifyInternal
}
