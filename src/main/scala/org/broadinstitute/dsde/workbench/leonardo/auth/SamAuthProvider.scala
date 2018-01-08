package org.broadinstitute.dsde.workbench.leonardo.auth

import akka.http.scaladsl.model.StatusCodes
import com.typesafe.config.Config
import com.typesafe.scalalogging.LazyLogging
import net.ceedubs.ficus.Ficus._
import io.swagger.client.ApiClient
import io.swagger.client.Configuration
import org.broadinstitute.dsde.workbench.leonardo.model.{LeoAuthProvider, LeoException, NotebookClusterActions, ProjectActions}
import org.broadinstitute.dsde.workbench.model.{UserInfo, WorkbenchEmail}
import io.swagger.client.api.ResourcesApi
import org.broadinstitute.dsde.workbench.leonardo.model.NotebookClusterActions._
import org.broadinstitute.dsde.workbench.leonardo.model.ProjectActions._
import org.broadinstitute.dsde.workbench.leonardo.model.Actions._
import org.broadinstitute.dsde.workbench.model.google.GoogleProject

import scala.concurrent.{ExecutionContext, Future}

class SamAuthProvider(authConfig: Config) extends LeoAuthProvider(authConfig) with LazyLogging {

  val notebookClusterResourceTypeName = "notebook-cluster"
  val billingProjectResourceTypeName = "billing-project"


  //is this how we do this???
  private[auth] def resourcesApi(userInfo: UserInfo): ResourcesApi = {
    val apiClient = new ApiClient()
    logger.info("USER ACCESS TOKEN:" + userInfo.accessToken.token)
    apiClient.setAccessToken(userInfo.accessToken.token)
    apiClient.setBasePath(authConfig.as[String]("samServer"))
    logger.info("API CLIENT:" + apiClient)
    new ResourcesApi(apiClient)
  }

  protected def getClusterResourceId(googleProject: String, clusterName: String): String = {
    googleProject + "_" + clusterName
  }

  //gets the string we want for each type of action - definitely NOT how we want to do this - probably put this is in a config and have ProjectAction
  // and NotebookClusterAction have a toString method. Doing it this way to test stuff quickly
  protected def getProjectActionString(action: Action): String = {
    action match {
      case projectAction: ProjectAction => getProjectActionString(projectAction)
      case notebookClusterAction: NotebookClusterAction => getNotebookClusterActionString(notebookClusterAction)
    }
  }

  protected def getProjectActionString(action: ProjectAction): String = {
    action match {
      case CreateClusters => "launch_notebook_cluster"
      case ListClusters => "list_notebook_cluster"
      case SyncDataToClusters => "sync_notebook_cluster"
      case DeleteClusters => "delete_notebook_cluster"
      case _ => "return error here???"
    }
  }

  protected def getNotebookClusterActionString(action: NotebookClusterAction): String = {
    action match {
      case GetClusterStatus => "status"
      case ConnectToCluster => "connect"
      case SyncDataToCluster => "sync"
      case DeleteCluster => "delete"
      case _ => "return error here???"
    }
  }


  case class AuthError(roles: String) extends
    LeoException(s"Roles for user: ", StatusCodes.Unauthorized)
  /**
    * @param userInfo The user in question
    * @param action The project-level action (above) the user is requesting
    * @param googleProject The Google project to check in
    * @return If the given user has permissions in this project to perform the specified action.
    */
  def hasProjectPermission(userInfo: UserInfo, action: ProjectActions.ProjectAction, googleProject: String)(implicit executionContext: ExecutionContext): Future[Boolean] = {
    val roles = resourcesApi(userInfo).resourceRoles("billing-project", "broad-dsde-dev").toString
    throw AuthError(roles)

   // Future{ resourcesApi(userInfo).resourceAction(billingProjectResourceTypeName, googleProject, getProjectActionString(action)) }
  }

  /**
    * Leo calls this method to verify if the user has permission to perform the given action on a specific notebook cluster.
    * It may call this method passing in a cluster that doesn't exist. Return Future.successful(false) if so.
    *
    * @param userInfo      The user in question
    * @param action        The cluster-level action (above) the user is requesting
    * @param googleProject The Google project the cluster was created in
    * @param clusterName   The user-provided name of the Dataproc cluster
    * @return If the userEmail has permission on this individual notebook cluster to perform this action
    */
  def hasNotebookClusterPermission(userInfo: UserInfo, action: NotebookClusterActions.NotebookClusterAction, googleProject: String, clusterName: String)(implicit executionContext: ExecutionContext): Future[Boolean] = {
    // get the id for the cluster resource
    val clusterResourceId = getClusterResourceId(googleProject, clusterName)

    // if action is connect, check only cluster resource. If action is anything else, either cluster or project must be true
    Future {
     if (action == ConnectToCluster) {
       resourcesApi(userInfo).resourceAction(notebookClusterResourceTypeName, clusterResourceId, getNotebookClusterActionString(action))
     } else {
       resourcesApi(userInfo).resourceAction(notebookClusterResourceTypeName, clusterResourceId, getNotebookClusterActionString(action)) ||
       resourcesApi(userInfo).resourceAction(billingProjectResourceTypeName, googleProject, getNotebookClusterActionString(action))
     }
    }
  }

  //Notifications that Leo has created/destroyed clusters. Allows the auth provider to register things.

  /**
    * Leo calls this method to notify the auth provider that a new notebook cluster has been created.
    * The returned future should complete once the provider has finished doing any associated work.
    * Returning a failed Future will prevent the cluster from being created, and will call notifyClusterDeleted for the same cluster.
    * Leo will wait, so be timely!
    *
    * @param userInfo      The email address of the user in question
    * @param googleProject The Google project the cluster was created in
    * @param clusterName   The user-provided name of the Dataproc cluster
    * @return A Future that will complete when the auth provider has finished doing its business.
    */
  def notifyClusterCreated(userInfo: UserInfo, googleProject: String, clusterName: String)(implicit executionContext: ExecutionContext): Future[Unit] = {
    val clusterResourceId = getClusterResourceId(googleProject, clusterName)
    // Add the cluster resource with the user as owner
    Future { resourcesApi(userInfo).createResource(notebookClusterResourceTypeName, clusterResourceId) }
  }

  /**
    * Leo calls this method to notify the auth provider that a notebook cluster has been deleted.
    * The returned future should complete once the provider has finished doing any associated work.
    * Leo will wait, so be timely!
    *
    * @param userInfo      The email address of the user in question
    * @param googleProject The Google project the cluster was created in
    * @param clusterName   The user-provided name of the Dataproc cluster
    * @return A Future that will complete when the auth provider has finished doing its business.
    */
  def notifyClusterDeleted(userInfo: UserInfo, googleProject: String, clusterName: String)(implicit executionContext: ExecutionContext): Future[Unit] = {
    // get the id for the cluster resource
    val clusterResourceId = getClusterResourceId(googleProject, clusterName)
    // delete the resource
    Future{resourcesApi(userInfo).deleteResource(notebookClusterResourceTypeName, clusterResourceId)}
  }
}
