package org.broadinstitute.dsde.workbench.leonardo.auth

import com.typesafe.config.Config
import org.broadinstitute.dsde.workbench.leonardo.model._
import org.broadinstitute.dsde.workbench.leonardo.model.google._
import org.broadinstitute.dsde.workbench.model.{UserInfo, WorkbenchEmail}
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

  override def hasProjectPermission(userInfo: UserInfo, action: ProjectActions.ProjectAction, googleProject: GoogleProject)(implicit executionContext: ExecutionContext): Future[Boolean] = {
    Future.successful(projectPermissions(action))
  }

  override def hasNotebookClusterPermission(internalId: String, userInfo: UserInfo, action: NotebookClusterActions.NotebookClusterAction, googleProject: GoogleProject, clusterName: ClusterName)(implicit executionContext: ExecutionContext): Future[Boolean] = {
    Future.successful(clusterPermissions(action))
  }

  override def filterUserVisibleClusters(userInfo: UserInfo, clusters: List[(GoogleProject, ClusterName)])(implicit executionContext: ExecutionContext): Future[List[(GoogleProject, ClusterName)]] = {
    if (canSeeClustersInAllProjects) {
      Future.successful(clusters)
    } else {
      Future.successful(clusters.filter { case (googleProject, _) =>
        canSeeAllClustersIn.contains(googleProject.value) || clusterPermissions(NotebookClusterActions.GetClusterStatus)
      })
    }
  }

  private def notifyInternal = {
    if( notifySucceeds )
      Future.successful(())
    else
      Future.failed(new RuntimeException("boom"))
  }

  override def notifyClusterCreated(internalId: String, creatorEmail: WorkbenchEmail, googleProject: GoogleProject, clusterName: ClusterName)(implicit executionContext: ExecutionContext): Future[Unit] = notifyInternal

  override def notifyClusterDeleted(internalId: String, userEmail: WorkbenchEmail, creatorEmail: WorkbenchEmail, googleProject: GoogleProject, clusterName: ClusterName)(implicit executionContext: ExecutionContext): Future[Unit] = notifyInternal

}
