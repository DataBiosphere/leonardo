package org.broadinstitute.dsde.workbench.leonardo.auth

import com.google.api.client.googleapis.auth.oauth2.GoogleCredential
import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport
import com.google.api.client.json.jackson2.JacksonFactory
import com.google.api.services.plus.PlusScopes
import com.typesafe.config.Config
import net.ceedubs.ficus.Ficus._
import io.swagger.client.ApiClient
import org.broadinstitute.dsde.workbench.leonardo.model._
import org.broadinstitute.dsde.workbench.model.WorkbenchEmail
import io.swagger.client.api.ResourcesApi
import io.swagger.client.api.GoogleApi
import org.broadinstitute.dsde.workbench.leonardo.model.NotebookClusterActions._
import org.broadinstitute.dsde.workbench.leonardo.model.ProjectActions._
import org.broadinstitute.dsde.workbench.leonardo.model.Actions._

import scala.collection.JavaConverters._
import java.io.{File, FileWriter}
import java.util.concurrent.TimeUnit

import akka.http.scaladsl.model.StatusCodes
import com.google.common.cache.{CacheBuilder, CacheLoader}

import scala.concurrent.{ExecutionContext, Future}

case class PetServiceAccountPerProject(petServiceAccount: WorkbenchEmail, googleProject: String)

case class NotebookActionError(action: Action) extends
  LeoException(s"${action.toString} was not recognized", StatusCodes.NotFound)


class SamAuthProvider(authConfig: Config, serviceAccountProvider: ServiceAccountProvider) extends LeoAuthProvider(authConfig, serviceAccountProvider) {
  private val notebookClusterResourceTypeName = "notebook-cluster"
  private val billingProjectResourceTypeName = "billing-project"

  private val httpTransport = GoogleNetHttpTransport.newTrustedTransport
  private val jsonFactory = JacksonFactory.getDefaultInstance
  private val saScopes = Seq(PlusScopes.USERINFO_EMAIL, PlusScopes.USERINFO_PROFILE)

  //Cache lookup of pet tokens
  val cacheExpiryTime = authConfig.getInt("cacheExpiryTime")
  val cacheMaxSize = authConfig.getInt("cacheMaxSize")

  private[leonardo] val petTokenCache = CacheBuilder.newBuilder()
    .expireAfterWrite(cacheExpiryTime, TimeUnit.MINUTES)
    .maximumSize(cacheMaxSize)
    .build(
      new CacheLoader[PetServiceAccountPerProject, String] {
        def load(petPerProject: PetServiceAccountPerProject) = {
          getPetAccessTokenFromSam(petPerProject.petServiceAccount, petPerProject.googleProject)
        }
      }
    )

  //Leo SA details -- needed to get pet keyfiles
  private val (leoEmail, leoPem) : (WorkbenchEmail, File) = serviceAccountProvider.getLeoServiceAccountAndKey

  //Given some credentials, gets an access token
  private def getAccessTokenUsingCredential(email: WorkbenchEmail, pem: File): String = {
    val credential = new GoogleCredential.Builder()
      .setTransport(httpTransport)
      .setJsonFactory(jsonFactory)
      .setServiceAccountId(leoEmail.value)
      .setServiceAccountScopes(saScopes.asJava)
      .setServiceAccountPrivateKeyFromPemFile(pem)
      .build()

    credential.refreshToken
    credential.getAccessToken
  }

  //"Slow" lookup of pet's access token. The cache calls this when it needs to.
  private def getPetAccessTokenFromSam(userEmail: WorkbenchEmail, googleProject: String): String = {
    val samAPI = googleApi(getAccessTokenUsingCredential(leoEmail, leoPem))
    val petServiceAccount = samAPI.getPetServiceAccount(googleProject)
    val serviceAccountKey = samAPI.getPetServiceAccountKey(googleProject)
    getAccessTokenUsingCredential(WorkbenchEmail(petServiceAccount), new java.io.File(serviceAccountKey.getPrivateKeyData))
  }

  //"Fast" lookup of pet's access token, using the cache.
  private def getCachedPetAccessToken(userEmail: WorkbenchEmail, googleProject: String): String = {
    petTokenCache.get(PetServiceAccountPerProject(userEmail, googleProject))
  }

  //A resources API if you already have a token
  private[auth] def resourcesApi(accessToken: String): ResourcesApi = {
    val apiClient = new ApiClient()
    apiClient.setAccessToken(accessToken)
    apiClient.setBasePath(authConfig.as[String]("samServer"))
    new ResourcesApi(apiClient)
  }

  //A resources API as the given user's pet SA
  private[auth] def resourcesApiAsPet(userEmail: WorkbenchEmail, googleProject: String): ResourcesApi = {
    resourcesApi(getCachedPetAccessToken(userEmail, googleProject))
  }

  private[auth] def googleApi(accessToken: String): GoogleApi = {
    val apiClient = new ApiClient()
    apiClient.setAccessToken(accessToken)
    apiClient.setBasePath(authConfig.as[String]("samServer"))
    new GoogleApi(apiClient)
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
      case _ => throw NotebookActionError(action)
    }
  }

  protected def getNotebookClusterActionString(action: NotebookClusterAction): String = {
    action match {
      case GetClusterStatus => "status"
      case ConnectToCluster => "connect"
      case SyncDataToCluster => "sync"
      case DeleteCluster => "delete"
      case _ => throw NotebookActionError(action)
    }
  }

  /**
    * @param userEmail The user in question
    * @param action The project-level action (above) the user is requesting
    * @param googleProject The Google project to check in
    * @return If the given user has permissions in this project to perform the specified action.
    */
  def hasProjectPermission(userEmail: WorkbenchEmail, action: ProjectActions.ProjectAction, googleProject: String)(implicit executionContext: ExecutionContext): Future[Boolean] = {
    Future{ resourcesApiAsPet(userEmail, googleProject).resourceAction(billingProjectResourceTypeName, googleProject, getProjectActionString(action)) }
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
  def hasNotebookClusterPermission(userEmail: WorkbenchEmail, action: NotebookClusterActions.NotebookClusterAction, googleProject: String, clusterName: String)(implicit executionContext: ExecutionContext): Future[Boolean] = {
    // get the id for the cluster resource
    val clusterResourceId = getClusterResourceId(googleProject, clusterName)

    // if action is connect, check only cluster resource. If action is anything else, either cluster or project must be true
    Future {
      val notebookAction = resourcesApiAsPet(userEmail, googleProject).resourceAction(notebookClusterResourceTypeName, clusterResourceId, getNotebookClusterActionString(action))
      if (action == ConnectToCluster) {
        notebookAction
      } else {
        notebookAction || resourcesApiAsPet(userEmail, googleProject).resourceAction(billingProjectResourceTypeName, googleProject, getNotebookClusterActionString(action))
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
  def notifyClusterCreated(userEmail: WorkbenchEmail, googleProject: String, clusterName: String)(implicit executionContext: ExecutionContext): Future[Unit] = {
    val clusterResourceId = getClusterResourceId(googleProject, clusterName)
    // Add the cluster resource with the user as owner
    Future { resourcesApiAsPet(userEmail, googleProject).createResource(notebookClusterResourceTypeName, clusterResourceId) }
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
  def notifyClusterDeleted(userEmail: WorkbenchEmail, googleProject: String, clusterName: String)(implicit executionContext: ExecutionContext): Future[Unit] = {
    // get the id for the cluster resource
    val clusterResourceId = getClusterResourceId(googleProject, clusterName)
    // delete the resource
    Future{resourcesApiAsPet(userEmail, googleProject).deleteResource(notebookClusterResourceTypeName, clusterResourceId)}
  }
}
