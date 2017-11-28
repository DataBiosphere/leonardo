package org.broadinstitute.dsde.workbench.leonardo.auth

import java.util.UUID

import com.typesafe.config.Config
import net.ceedubs.ficus.Ficus._
import org.broadinstitute.dsde.workbench.leonardo.model.{GoogleProject, LeoAuthProvider, NotebookClusterAction, ProjectAction}
import org.broadinstitute.dsde.workbench.model.WorkbenchUserEmail

import scala.concurrent.Future

class WhitelistAuthProvider(authConfig: Config) extends LeoAuthProvider(authConfig) {

  val whitelist = authConfig.as[(Set[String])]("whitelist").map(WorkbenchUserEmail)

  def checkWhitelist(user: WorkbenchUserEmail): Future[Boolean] = {
    Future.successful(whitelist contains user)
  }

  //Does this user have permission in all projects to perform this action?
  def hasPermissionInAllProjects(user: WorkbenchUserEmail, action: ProjectAction): Future[Boolean] = {
    checkWhitelist(user)
  }

  //Does the user have permission in this project to perform this action?
  def hasProjectPermission(user: WorkbenchUserEmail, action: ProjectAction, project: GoogleProject): Future[Boolean] = {
    checkWhitelist(user)
  }

  //List the projects in which the user has ListClusters action.
  //If hasPermissionInAllProjects(_, ListClusters) returns true in your implementation, you should return Set.empty() for this function.
  def getProjectsWithListClustersPermission(user: WorkbenchUserEmail): Future[Set[GoogleProject]] = {
    Future.successful(Set.empty[GoogleProject])
  }

  //Does the user have permission on this individual notebook cluster to perform this action?
  def hasNotebookClusterPermission(user: WorkbenchUserEmail, action: NotebookClusterAction, clusterGoogleID: UUID): Future[Boolean] = {
    checkWhitelist(user)
  }

  //Notifications that Leo has created/destroyed clusters.
  //The resulting future should return once the provider has finished doing any associated work. Leo will wait.
  def notifyClusterCreated(user: WorkbenchUserEmail, project: GoogleProject, clusterGoogleID: UUID): Future[Unit] = Future.successful(())
  def notifyClusterDestroyed(user: WorkbenchUserEmail, project: GoogleProject, clusterGoogleID: UUID): Future[Unit] = Future.successful(())
}
