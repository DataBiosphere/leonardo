package org.broadinstitute.dsde.workbench.leonardo.auth

import java.util.UUID

import com.typesafe.config.Config
import net.ceedubs.ficus.Ficus._
import org.broadinstitute.dsde.workbench.leonardo.model.LeoAuthProvider
import org.broadinstitute.dsde.workbench.leonardo.model.NotebookClusterActions.NotebookClusterAction
import org.broadinstitute.dsde.workbench.leonardo.model.ProjectActions.ProjectAction
import org.broadinstitute.dsde.workbench.model.{UserInfo, WorkbenchEmail}

import scala.concurrent.Future

class WhitelistAuthProvider(authConfig: Config) extends LeoAuthProvider(authConfig) {

  val whitelist = authConfig.as[(Set[String])]("whitelist").map(_.toLowerCase)

  protected def checkWhitelist(userEmail: WorkbenchEmail): Future[Boolean] = {
    Future.successful(whitelist contains userEmail.value.toLowerCase)
  }

  /**
    * @param userInfo The user in question
    * @param action The project-level action (above) the user is requesting
    * @param googleProject The Google project to check in
    * @return If the given user has permissions in this project to perform the specified action.
    */
  def hasProjectPermission(userInfo: UserInfo, action: ProjectAction, googleProject: String): Future[Boolean]  = {
    checkWhitelist(userInfo.userEmail)
  }

  /**
    * @param userInfo The user in question
    * @param action   The cluster-level action (above) the user is requesting
    * @param clusterName The user-provided name of the Dataproc cluster
    * @return If the userEmail has permission on this individual notebook cluster to perform this action
    */
  def hasNotebookClusterPermission(userInfo: UserInfo, action: NotebookClusterAction, googleProject: String, clusterName: String): Future[Boolean]  = {
    checkWhitelist(userInfo.userEmail)
  }

  //Notifications that Leo has created/destroyed clusters. Allows the auth provider to register things.

  /**
    * Leo calls this method to notify the auth provider that a new notebook cluster has been created.
    * The returned future should complete once the provider has finished doing any associated work.
    * Leo will wait, so be timely!
    *
    * @param userEmail     The email address of the user in question
    * @param googleProject The Google project the cluster was created in
    * @param clusterName   The user-provided name of the Dataproc cluster
    * @return A Future that will complete when the auth provider has finished doing its business.
    */
  def notifyClusterCreated(userEmail: String, googleProject: String, clusterName: String): Future[Unit] = Future.successful(())

  /**
    * Leo calls this method to notify the auth provider that a notebook cluster has been destroyed.
    * The returned future should complete once the provider has finished doing any associated work.
    * Leo will wait, so be timely!
    *
    * @param userEmail     The email address of the user in question
    * @param googleProject The Google project the cluster was created in
    * @param clusterName   The user-provided name of the Dataproc cluster
    * @return A Future that will complete when the auth provider has finished doing its business.
    */
  def notifyClusterDeleted(userEmail: String, googleProject: String, clusterName: String): Future[Unit] = Future.successful(())
}
