package org.broadinstitute.dsde.workbench.leonardo.auth

import com.google.api.client.auth.oauth2.Credential
import com.google.api.client.googleapis.auth.oauth2.GoogleCredential
import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport
import com.google.api.client.json.jackson2.JacksonFactory
import com.google.api.services.plus.PlusScopes
import com.typesafe.config.Config
import com.typesafe.scalalogging.LazyLogging
import net.ceedubs.ficus.Ficus._
import io.swagger.client.ApiClient
import org.broadinstitute.dsde.workbench.leonardo.model.{LeoAuthProvider, NotebookClusterActions, ProjectActions, ServiceAccountProvider}
import org.broadinstitute.dsde.workbench.model.{UserInfo, WorkbenchEmail}
import io.swagger.client.api.ResourcesApi
import org.broadinstitute.dsde.workbench.leonardo.model.NotebookClusterActions._
import org.broadinstitute.dsde.workbench.leonardo.model.ProjectActions._
import org.broadinstitute.dsde.workbench.leonardo.model.Actions._
import org.broadinstitute.dsde.workbench.model.google.GoogleProject

import scala.collection.JavaConverters._
import java.io.File
import java.time.Instant
import java.util.concurrent.TimeUnit

import com.google.common.cache.{CacheBuilder, CacheLoader}

import scala.concurrent.{ExecutionContext, Future}

class SamAuthProvider(authConfig: Config, serviceAccountProvider: ServiceAccountProvider) extends LeoAuthProvider(authConfig, serviceAccountProvider) with LazyLogging {

  val notebookClusterResourceTypeName = "notebook-cluster"
  val billingProjectResourceTypeName = "billing-project"

  private val httpTransport = GoogleNetHttpTransport.newTrustedTransport
  private val jsonFactory = JacksonFactory.getDefaultInstance
  val saScopes = Seq(PlusScopes.USERINFO_EMAIL, PlusScopes.USERINFO_PROFILE)

  //Cache lookup of pet tokens
  val cacheExpiryTime = authConfig.getInt("cacheExpiryTime")
  val cacheMaxSize = authConfig.getInt("cacheMaxSize")

  private[leonardo] val petTokenCache = CacheBuilder.newBuilder()
    .expireAfterWrite(cacheExpiryTime, TimeUnit.MINUTES)
    .maximumSize(cacheMaxSize)
    .build(
      new CacheLoader[WorkbenchEmail, String] {
        def load(userEmail: WorkbenchEmail) = {
          getPetAccessTokenFromSam(userEmail)
        }
      }
    )

  //Leo SA details -- needed to get pet keyfiles
  val (leoEmail, leoPem) : (WorkbenchEmail, File) = serviceAccountProvider.getLeoServiceAccountAndKey

  //Given some credentials, gets an access token
  private def getAccessTokenUsingCredential(email: WorkbenchEmail, pem: File): String = {
    val credential = new GoogleCredential.Builder()
      .setTransport(httpTransport)
      .setJsonFactory(jsonFactory)
      .setServiceAccountId(leoEmail.value)
      .setServiceAccountScopes(saScopes.asJava)
      .setServiceAccountPrivateKeyFromPemFile(leoPem)
      .build()

    credential.refreshToken
    credential.getAccessToken
  }

  //"Slow" lookup of pet's access token. The cache calls this when it needs to.
  private def getPetAccessTokenFromSam(userEmail: WorkbenchEmail): String = {
    val samAPI = resourcesApi(getAccessTokenUsingCredential(leoEmail, leoPem))
    val (petEmail, petKey): (WorkbenchEmail, File) = samAPI.gimmeThisUsersPetsKey(userEmail)
    getAccessTokenUsingCredential(petEmail, petKey)
  }

  //"Fast" lookup of pet's access token, using the cache.
  private def getCachedPetAccessToken(userEmail: WorkbenchEmail): String = {
    petTokenCache.get(userEmail)
  }

  //A resources API if you already have a token
  private[auth] def resourcesApi(accessToken: String): ResourcesApi = {
    val apiClient = new ApiClient()
    apiClient.setAccessToken(accessToken)
    apiClient.setBasePath(authConfig.as[String]("samServer"))
    new ResourcesApi(apiClient)
  }

  //A resources API as the given user's pet SA
  private[auth] def resourcesApiAsPet(userEmail: WorkbenchEmail): ResourcesApi = {
    resourcesApi(getCachedPetAccessToken(userEmail))
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

  /**
    * @param userInfo The user in question
    * @param action The project-level action (above) the user is requesting
    * @param googleProject The Google project to check in
    * @return If the given user has permissions in this project to perform the specified action.
    */
  def hasProjectPermission(userInfo: UserInfo, action: ProjectActions.ProjectAction, googleProject: String)(implicit executionContext: ExecutionContext): Future[Boolean] = {
    Future{ resourcesApi(userInfo).resourceAction(billingProjectResourceTypeName, googleProject, getProjectActionString(action)) }
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
