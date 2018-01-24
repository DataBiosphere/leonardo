package org.broadinstitute.dsde.workbench.leonardo.auth

import com.typesafe.config.Config
import org.broadinstitute.dsde.workbench.leonardo.model._
import org.broadinstitute.dsde.workbench.model.WorkbenchEmail

import scala.concurrent.{ExecutionContext, Future}

class MockLeoAuthProvider(authConfig: Config, serviceAccountProvider: ServiceAccountProvider, notifySucceeds: Boolean = true) extends LeoAuthProvider(authConfig, serviceAccountProvider) {
  //behaviour defined in test\...\reference.conf
  val projectPermissions: Map[ProjectActions.ProjectAction, Boolean] =
    (ProjectActions.allActions map (action => action -> authConfig.getBoolean(action.toString) )).toMap
  val clusterPermissions: Map[NotebookClusterActions.NotebookClusterAction, Boolean] =
    (NotebookClusterActions.allActions map (action => action -> authConfig.getBoolean(action.toString) )).toMap

  def hasProjectPermission(userEmail: WorkbenchEmail, action: ProjectActions.ProjectAction, googleProject: String)(implicit executionContext: ExecutionContext): Future[Boolean] = {
    Future.successful(projectPermissions(action))
  }

  def hasNotebookClusterPermission(userEmail: WorkbenchEmail, action: NotebookClusterActions.NotebookClusterAction, googleProject: String, clusterName: String)(implicit executionContext: ExecutionContext): Future[Boolean] = {
    Future.successful(clusterPermissions(action))
  }

  private def notifyInternal = {
    if( notifySucceeds )
      Future.successful(())
    else
      Future.failed(new RuntimeException("boom"))
  }

  def notifyClusterCreated(userEmail: WorkbenchEmail, googleProject: String, clusterName: String)(implicit executionContext: ExecutionContext): Future[Unit] = notifyInternal

  def notifyClusterDeleted(userEmail: WorkbenchEmail, googleProject: String, clusterName: String)(implicit executionContext: ExecutionContext): Future[Unit] = notifyInternal
}
