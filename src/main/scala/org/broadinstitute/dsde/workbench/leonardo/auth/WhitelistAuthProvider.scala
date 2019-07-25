package org.broadinstitute.dsde.workbench.leonardo.auth

import cats.implicits._
import com.typesafe.config.Config
import net.ceedubs.ficus.Ficus._
import org.broadinstitute.dsde.workbench.leonardo.model.{LeoAuthProvider, ServiceAccountProvider}
import org.broadinstitute.dsde.workbench.leonardo.model.google.ClusterName
import org.broadinstitute.dsde.workbench.leonardo.model.NotebookClusterActions.NotebookClusterAction
import org.broadinstitute.dsde.workbench.leonardo.model.ProjectActions.ProjectAction
import org.broadinstitute.dsde.workbench.model.{UserInfo, WorkbenchEmail}
import org.broadinstitute.dsde.workbench.model.google.GoogleProject

import scala.concurrent.{ExecutionContext, Future}

class WhitelistAuthProvider(config: Config, serviceAccountProvider: ServiceAccountProvider) extends LeoAuthProvider(config, serviceAccountProvider) {

  val whitelist = config.as[Set[String]]("whitelist").map(_.toLowerCase)

  protected def checkWhitelist(userInfo: UserInfo): Future[Boolean] = {
    Future.successful(whitelist contains userInfo.userEmail.value.toLowerCase)
  }

  /**
    * @param userInfo The user in question
    * @param action The project-level action (above) the user is requesting
    * @param googleProject The Google project to check in
    * @return If the given user has permissions in this project to perform the specified action.
    */
  override def hasProjectPermission(userInfo: UserInfo, action: ProjectAction, googleProject: GoogleProject)(implicit executionContext: ExecutionContext): Future[Boolean]  = {
    checkWhitelist(userInfo)
  }

  /**
    * @param internalId     The internal ID for the cluster (i.e. used for Sam resources)
    * @param userInfo The user in question
    * @param action   The cluster-level action (above) the user is requesting
    * @param clusterName The user-provided name of the Dataproc cluster
    * @return If the userEmail has permission on this individual notebook cluster to perform this action
    */
  override def hasNotebookClusterPermission(internalId: String, userInfo: UserInfo, action: NotebookClusterAction, googleProject: GoogleProject, clusterName: ClusterName)(implicit executionContext: ExecutionContext): Future[Boolean]  = {
    checkWhitelist(userInfo)
  }

  /**
    * Leo calls this method when it receives a "list clusters" API call, passing in all non-deleted clusters from the database.
    * It should return a list of clusters that the user can see according to their authz.
    *
    * @param userInfo The user in question
    * @param clusters All non-deleted clusters from the database
    * @return         Filtered list of clusters that the user is allowed to see
    */
  override def filterUserVisibleClusters(userInfo: UserInfo, clusters: List[(GoogleProject, ClusterName)])(implicit executionContext: ExecutionContext): Future[List[(GoogleProject, ClusterName)]] = {
    clusters.traverseFilter { a =>
      checkWhitelist(userInfo).map {
        case true => Some(a)
        case false => None
      }
    }
  }


  //Notifications that Leo has created/destroyed clusters. Allows the auth provider to register things.

  /**
    * Leo calls this method to notify the auth provider that a new notebook cluster has been created.
    * The returned future should complete once the provider has finished doing any associated work.
    * Leo will wait, so be timely!
    *
    * @param internalId     The internal ID for the cluster (i.e. used for Sam resources)
    * @param creatorEmail     The email address of the user in question
    * @param googleProject The Google project the cluster was created in
    * @param clusterName   The user-provided name of the Dataproc cluster
    * @return A Future that will complete when the auth provider has finished doing its business.
    */
  def notifyClusterCreated(internalId: String, creatorEmail: WorkbenchEmail, googleProject: GoogleProject, clusterName: ClusterName)(implicit executionContext: ExecutionContext): Future[Unit] = Future.successful(())

  /**
    * Leo calls this method to notify the auth provider that a notebook cluster has been destroyed.
    * The returned future should complete once the provider has finished doing any associated work.
    * Leo will wait, so be timely!
    *
    * @param internalId     The internal ID for the cluster (i.e. used for Sam resources)
    * @param userEmail     The email address of the user in question
    * @param creatorEmail     The email address of the creator of the cluster
    * @param googleProject The Google project the cluster was created in
    * @param clusterName   The user-provided name of the Dataproc cluster
    * @return A Future that will complete when the auth provider has finished doing its business.
    */
  def notifyClusterDeleted(internalId: String, userEmail: WorkbenchEmail, creatorEmail: WorkbenchEmail, googleProject: GoogleProject, clusterName: ClusterName)(implicit executionContext: ExecutionContext): Future[Unit] = Future.successful(())

}
