package org.broadinstitute.dsde.workbench.leonardo.auth

import com.typesafe.config.Config
import org.broadinstitute.dsde.workbench.leonardo.model._
import org.broadinstitute.dsde.workbench.model.WorkbenchEmail
import org.broadinstitute.dsde.workbench.model.google.GoogleProject
import net.ceedubs.ficus.Ficus._

import scala.concurrent.{ExecutionContext, Future}

class MockLeoAuthProvider(authConfig: Config, serviceAccountProvider: ServiceAccountProvider, notifySucceeds: Boolean = true) extends LeoAuthProvider(authConfig, serviceAccountProvider) {
  //behaviour defined in test\...\reference.conf
  val projectPermissions: Map[ProjectActions.ProjectAction, Boolean] =
    (ProjectActions.allActions map (action => action -> authConfig.getBoolean(action.toString) )).toMap
  val clusterPermissions: Map[NotebookClusterActions.NotebookClusterAction, Boolean] =
    (NotebookClusterActions.allActions map (action => action -> authConfig.getBoolean(action.toString) )).toMap

  val canSeeClustersInAllProjects = authConfig.as[Option[Boolean]]("canSeeClustersInAllProjects").getOrElse(false)
  val canSeeAllClustersIn = authConfig.as[Option[Seq[String]]]("canSeeAllClustersIn").getOrElse(Seq.empty)

  override def canSeeAllClustersInProject(userEmail: WorkbenchEmail, googleProject: GoogleProject)(implicit executionContext: ExecutionContext): Future[Boolean] = {
    if(canSeeClustersInAllProjects) {
      Future.successful(true)
    } else {
      Future.successful( canSeeAllClustersIn.contains(googleProject.value) )
    }
  }

  def hasProjectPermission(userEmail: WorkbenchEmail, action: ProjectActions.ProjectAction, googleProject: GoogleProject)(implicit executionContext: ExecutionContext): Future[Boolean] = {
    Future.successful(projectPermissions(action))
  }

  def hasNotebookClusterPermission(userEmail: WorkbenchEmail, action: NotebookClusterActions.NotebookClusterAction, googleProject: GoogleProject, clusterName: ClusterName)(implicit executionContext: ExecutionContext): Future[Boolean] = {
    Future.successful(clusterPermissions(action))
  }

  private def notifyInternal = {
    if( notifySucceeds )
      Future.successful(())
    else
      Future.failed(new RuntimeException("boom"))
  }

  def notifyClusterCreated(userEmail: WorkbenchEmail, googleProject: GoogleProject, clusterName: ClusterName)(implicit executionContext: ExecutionContext): Future[Unit] = notifyInternal

  def notifyClusterDeleted(userEmail: WorkbenchEmail, creatorEmail: WorkbenchEmail, googleProject: GoogleProject, clusterName: ClusterName)(implicit executionContext: ExecutionContext): Future[Unit] = notifyInternal
}
