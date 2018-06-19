package org.broadinstitute.dsde.workbench.leonardo.auth

import cats.implicits._
import akka.actor.ActorSystem
import com.typesafe.config.Config
import net.ceedubs.ficus.Ficus._
import org.broadinstitute.dsde.workbench.google.GoogleCredentialModes.Token
import org.broadinstitute.dsde.workbench.google.{GoogleIamDAO, HttpGoogleIamDAO}
import org.broadinstitute.dsde.workbench.leonardo.model.NotebookClusterActions.NotebookClusterAction
import org.broadinstitute.dsde.workbench.leonardo.model.ProjectActions.ProjectAction
import org.broadinstitute.dsde.workbench.leonardo.model.google.ClusterName
import org.broadinstitute.dsde.workbench.leonardo.model.{LeoAuthProvider, ServiceAccountProvider}
import org.broadinstitute.dsde.workbench.model.{UserInfo, WorkbenchEmail}
import org.broadinstitute.dsde.workbench.model.google.{GoogleProject, IamPermission}

import scala.concurrent.{ExecutionContext, Future}

class IamProxyAuthProvider(config: Config, serviceAccountProvider: ServiceAccountProvider)(implicit val system: ActorSystem) extends LeoAuthProvider(config, serviceAccountProvider) {

  val applicationName: String = config.getValue("applicationName").toString()
  val requiredPermissions: Set[IamPermission] = config.as[Set[String]]("requiredProjectIamPermissions").map(p => IamPermission(p))

  protected def petGoogleIamDao(token: String)(implicit executionContext: ExecutionContext): GoogleIamDAO = {
    new HttpGoogleIamDAO(applicationName, Token(() => token), "google")
  }

  protected def checkUserAccess(userInfo: UserInfo, googleProject: GoogleProject)(implicit executionContext: ExecutionContext): Future[Boolean] = {
    val iamDAO: GoogleIamDAO = petGoogleIamDao(userInfo.accessToken.token)
    val foundPermissions = iamDAO.testIamPermission(googleProject, requiredPermissions)
    Future.successful(requiredPermissions == foundPermissions)
  }

  /**
    * @param userInfo The user in question
    * @param action The project-level action (above) the user is requesting
    * @param googleProject The Google project to check in
    * @return If the given user has permissions in this project to perform the specified action.
    */
  override def hasProjectPermission(userInfo: UserInfo, action: ProjectAction, googleProject: GoogleProject)(implicit executionContext: ExecutionContext): Future[Boolean]  = {
    checkUserAccess(userInfo, googleProject)
  }

  /**
    * @param userInfo The user in question
    * @param action   The cluster-level action (above) the user is requesting
    * @param clusterName The user-provided name of the Dataproc cluster
    * @return If the userEmail has permission on this individual notebook cluster to perform this action
    */
  override def hasNotebookClusterPermission(userInfo: UserInfo, action: NotebookClusterAction, googleProject: GoogleProject, clusterName: ClusterName)(implicit executionContext: ExecutionContext): Future[Boolean]  = {
    checkUserAccess(userInfo, googleProject)
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
    // Check each project for user-access exactly once, then filter by project.
    val projects = clusters.map(lv => lv._1).toSet.toList
    val projectAccess = projects.map(p => p.value -> checkUserAccess(userInfo, p)).toMap
    clusters.traverseFilter { c =>
      projectAccess(c._1.value).map {
        case true => Some(c)
        case false => None
      }
    }
  }

  /**
    * Leo calls this method to notify the auth provider that a new notebook cluster has been created.
    * The returned future should complete once the provider has finished doing any associated work.
    * Leo will wait, so be timely!
    *
    * @param creatorEmail     The email address of the user in question
    * @param googleProject The Google project the cluster was created in
    * @param clusterName   The user-provided name of the Dataproc cluster
    * @return A Future that will complete when the auth provider has finished doing its business.
    */
  def notifyClusterCreated(creatorEmail: WorkbenchEmail, googleProject: GoogleProject, clusterName: ClusterName)(implicit executionContext: ExecutionContext): Future[Unit] = Future.successful(())

  /**
    * Leo calls this method to notify the auth provider that a notebook cluster has been destroyed.
    * The returned future should complete once the provider has finished doing any associated work.
    * Leo will wait, so be timely!
    *
    * @param userEmail     The email address of the user in question
    * @param creatorEmail     The email address of the creator of the cluster
    * @param googleProject The Google project the cluster was created in
    * @param clusterName   The user-provided name of the Dataproc cluster
    * @return A Future that will complete when the auth provider has finished doing its business.
    */
  def notifyClusterDeleted(userEmail: WorkbenchEmail, creatorEmail: WorkbenchEmail, googleProject: GoogleProject, clusterName: ClusterName)(implicit executionContext: ExecutionContext): Future[Unit] = Future.successful(())

}
