package org.broadinstitute.dsde.workbench.leonardo.auth

import com.typesafe.config.Config
import com.typesafe.scalalogging.LazyLogging
import net.ceedubs.ficus.Ficus._
import java.io.File

import akka.http.scaladsl.model.StatusCodes
import org.broadinstitute.dsde.workbench.leonardo.model.Actions.Action
import org.broadinstitute.dsde.workbench.leonardo.model._
import org.broadinstitute.dsde.workbench.leonardo.model.NotebookClusterActions._
import org.broadinstitute.dsde.workbench.leonardo.model.ProjectActions.CreateClusters
import org.broadinstitute.dsde.workbench.model.WorkbenchEmail
import org.broadinstitute.dsde.workbench.model.google.GoogleProject

import scala.concurrent.{ExecutionContext, Future}

case class NotebookActionError(action: Action) extends
  LeoException(s"${action.toString} was not recognized", StatusCodes.NotFound)

class SamAuthProvider(authConfig: Config, serviceAccountProvider: ServiceAccountProvider) extends LeoAuthProvider(authConfig, serviceAccountProvider) with LazyLogging {


  //Leo SA details -- needed to get pet keyfiles
  private val (leoEmail, leoPem) : (WorkbenchEmail, File) = serviceAccountProvider.getLeoServiceAccountAndKey

  val samClient = new SwaggerSamClient(authConfig.getString("samServer"), authConfig.getInt("cacheExpiryTime"), authConfig.getInt("cacheMaxSize"), leoEmail, leoPem)

   def samAPI: SwaggerSamClient = samClient

  //gets the string we want for each type of action - definitely NOT how we want to do this in the long run
  protected def getProjectActionString(action: Action): String = {
    projectActionMap.getOrElse(action, throw NotebookActionError(action))
  }

  //gets the string we want for each type of action - definitely NOT how we want to do this in the long run
  protected def getNotebookActionString(action: Action): String = {
    notebookActionMap.getOrElse(action, throw NotebookActionError(action))
  }

  private def projectActionMap: Map[Action, String] = Map(
    //list_notebook_cluster
    GetClusterStatus -> "list_notebook_cluster",
    CreateClusters -> "launch_notebook_cluster",
    SyncDataToCluster -> "sync_notebook_cluster",
    DeleteCluster -> "delete_notebook_cluster")


  private def notebookActionMap: Map[Action, String] = Map(
    GetClusterStatus -> "status",
    ConnectToCluster -> "connect",
    SyncDataToCluster -> "sync",
    DeleteCluster -> "delete")


  /**
    * @param userEmail The user in question
    * @param action The project-level action (above) the user is requesting
    * @param googleProject The Google project to check in
    * @return If the given user has permissions in this project to perform the specified action.
    */
  def hasProjectPermission(userEmail: WorkbenchEmail, action: ProjectActions.ProjectAction, googleProject: GoogleProject)(implicit executionContext: ExecutionContext): Future[Boolean] = {
    Future { samAPI.hasActionOnBillingProjectResource(userEmail,googleProject, getProjectActionString(action)) }
  }



  /**
    * When listing clusters, Leo will perform a GROUP BY on google projects and call this function once per google project.
    * If you have an implementation such that users, even in some cases, can see all clusters in a google project, overriding
    * this function may lead to significant performance improvements.
    * For any projects where this function call returns Future.successful(false), Leo will then call hasNotebookClusterPermission
    * for every cluster in that project, passing in action = GetClusterStatus.
    *
    * @param userEmail The user in question
    * @param googleProject A Google project
    * @return If the given user can see all clusters in this project
    */
 override def canSeeAllClustersInProject(userEmail: WorkbenchEmail, googleProject: GoogleProject)(implicit executionContext: ExecutionContext): Future[Boolean] = {
   Future { samAPI.hasActionOnBillingProjectResource(userEmail,googleProject, "list_notebook_cluster") }

   //hasProjectPermission(userEmail, "list_notebook_cluster", googleProject)
  }


  /**
    * Leo calls this method to verify if the user has permission to perform the given action on a specific notebook cluster.
    * It may call this method passing in a cluster that doesn't exist. Return Future.successful(false) if so.
    *
    * @param userEmail      The user in question
    * @param action        The cluster-level action (above) the user is requesting
    * @param googleProject The Google project the cluster was created in
    * @param clusterName   The user-provided name of the Dataproc cluster
    * @return If the userEmail has permission on this individual notebook cluster to perform this action
    */
  def hasNotebookClusterPermission(userEmail: WorkbenchEmail, action: NotebookClusterActions.NotebookClusterAction, googleProject: GoogleProject, clusterName: ClusterName)(implicit executionContext: ExecutionContext): Future[Boolean] = {
    // if action is connect, check only cluster resource. If action is anything else, either cluster or project must be true
    Future {
      val notebookAction = samAPI.hasActionOnNotebookClusterResource(userEmail,googleProject,clusterName, getNotebookActionString(action))
      if (action == ConnectToCluster) {
        notebookAction
      } else {
        val projectAction = samAPI.hasActionOnBillingProjectResource(userEmail,googleProject, getProjectActionString(action))
        notebookAction || projectAction   //resourcesApiAsPet(userEmail, googleProject).resourceAction(billingProjectResourceTypeName, googleProject.value, getActionString(action))
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
    * @param userEmail      The email address of the user in question
    * @param googleProject The Google project the cluster was created in
    * @param clusterName   The user-provided name of the Dataproc cluster
    * @return A Future that will complete when the auth provider has finished doing its business.
    */
  def notifyClusterCreated(userEmail: WorkbenchEmail, googleProject: GoogleProject, clusterName: ClusterName)(implicit executionContext: ExecutionContext): Future[Unit] = {
    // Add the cluster resource with the user as owner
    Future { samAPI.createNotebookClusterResource(userEmail, googleProject, clusterName) }
  }

  /**
    * Leo calls this method to notify the auth provider that a notebook cluster has been deleted.
    * The returned future should complete once the provider has finished doing any associated work.
    * Leo will wait, so be timely!
    *
    * @param userEmail      The email address of the user in question
    * @param googleProject The Google project the cluster was created in
    * @param clusterName   The user-provided name of the Dataproc cluster
    * @return A Future that will complete when the auth provider has finished doing its business.
    */
  def notifyClusterDeleted(userEmail: WorkbenchEmail, googleProject: GoogleProject, clusterName: ClusterName)(implicit executionContext: ExecutionContext): Future[Unit] = {
    // get the id for the cluster resource
    // delete the resource
    Future{ samAPI.deleteNotebookClusterResource(userEmail, googleProject, clusterName) }
  }
}
