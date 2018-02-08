package org.broadinstitute.dsde.workbench.leonardo.auth

import java.io.{ByteArrayInputStream, File}
import java.util.concurrent.TimeUnit

import com.google.api.client.googleapis.auth.oauth2.GoogleCredential
import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport
import com.google.api.client.json.jackson2.JacksonFactory
import com.google.api.services.plus.PlusScopes
import com.google.auth.oauth2.ServiceAccountCredentials
import com.google.common.cache.{CacheBuilder, CacheLoader}
import com.typesafe.scalalogging.LazyLogging
import io.swagger.client.ApiClient
import io.swagger.client.api.{GoogleApi, ResourcesApi}
import org.broadinstitute.dsde.workbench.leonardo.model.google.ClusterName
import org.broadinstitute.dsde.workbench.model.WorkbenchEmail
import org.broadinstitute.dsde.workbench.model.google.GoogleProject

import scala.collection.JavaConverters._
import scala.concurrent.duration.FiniteDuration

case class UserEmailAndProject(userEmail: WorkbenchEmail, googleProject: GoogleProject)

class SwaggerSamClient(samBasePath: String, cacheExpiryTime: FiniteDuration, cacheMaxSize: Int, leoEmail: WorkbenchEmail, leoPem: File) extends LazyLogging {

  private val httpTransport = GoogleNetHttpTransport.newTrustedTransport
  private val jsonFactory = JacksonFactory.getDefaultInstance
  private val saScopes = Seq(PlusScopes.USERINFO_EMAIL, PlusScopes.USERINFO_PROFILE)

  private val notebookClusterResourceTypeName = "notebook-cluster"
  private val billingProjectResourceTypeName = "billing-project"


  private[auth] def samGoogleApi(accessToken: String): GoogleApi = {
    val apiClient = new ApiClient()
    apiClient.setAccessToken(accessToken)
    apiClient.setBasePath(samBasePath)
    new GoogleApi(apiClient)
  }

  //A resources API if you already have a token
  private[auth] def samResourcesApi(accessToken: String): ResourcesApi = {
    val apiClient = new ApiClient()
    apiClient.setAccessToken(accessToken)
    apiClient.setBasePath(samBasePath)
    new ResourcesApi(apiClient)
  }

  //A resources API as the given user's pet SA
  private[auth] def resourcesApiAsPet(userEmail: WorkbenchEmail, googleProject: GoogleProject): ResourcesApi = {
    samResourcesApi(getCachedPetAccessToken(userEmail, googleProject))
  }

  //A google API as the given user's pet SA
  private[auth] def googleApiAsPet(userEmail: WorkbenchEmail, googleProject: GoogleProject): GoogleApi = {
    samGoogleApi(getCachedPetAccessToken(userEmail, googleProject))
  }

  //"Fast" lookup of pet's access token, using the cache.
  private def getCachedPetAccessToken(userEmail: WorkbenchEmail, googleProject: GoogleProject): String = {
    petTokenCache.get(UserEmailAndProject(userEmail, googleProject))
  }

  private[leonardo] val petTokenCache = CacheBuilder.newBuilder()
    .expireAfterWrite(cacheExpiryTime.toMinutes, TimeUnit.MINUTES)
    .maximumSize(cacheMaxSize)
    .build(
      new CacheLoader[UserEmailAndProject, String] {
        def load(userEmailAndProject: UserEmailAndProject) = {
          val token = getPetAccessTokenFromSam(userEmailAndProject.userEmail, userEmailAndProject.googleProject)
          token
        }
      }
    )

  //"Slow" lookup of pet's access token. The cache calls this when it needs to.
  private def getPetAccessTokenFromSam(userEmail: WorkbenchEmail, googleProject: GoogleProject): String = {
    val samAPI = samGoogleApi(getAccessTokenUsingPem(leoEmail, leoPem))
    val userPetServiceAccountKey = samAPI.getUserPetServiceAccountKey(googleProject.value, userEmail.value)
    getAccessTokenUsingJson(userPetServiceAccountKey)
  }

  //Given a pem, gets an access token
  private def getAccessTokenUsingPem(email: WorkbenchEmail, pem: File): String = {
    val credential = new GoogleCredential.Builder()
      .setTransport(httpTransport)
      .setJsonFactory(jsonFactory)
      .setServiceAccountId(email.value)
      .setServiceAccountScopes(saScopes.asJava)
      .setServiceAccountPrivateKeyFromPemFile(pem)
      .build()

    credential.refreshToken
    credential.getAccessToken
  }


  //Given some JSON, gets an access token
  private def getAccessTokenUsingJson(saKey: String) : String = {
    val keyStream = new ByteArrayInputStream(saKey.getBytes)
    val credential = ServiceAccountCredentials.fromStream(keyStream).createScoped(saScopes.asJava)
    credential.refreshAccessToken.getTokenValue
  }


  private def getClusterResourceId(googleProject: GoogleProject, clusterName: ClusterName): String = {
    googleProject.value + "_" + clusterName.value
  }


  def createNotebookClusterResource(userEmail: WorkbenchEmail, googleProject: GoogleProject, clusterName: ClusterName) = {
    resourcesApiAsPet(userEmail, googleProject).createResource(notebookClusterResourceTypeName, getClusterResourceId(googleProject, clusterName))
  }

  def deleteNotebookClusterResource(creatorEmail: WorkbenchEmail, googleProject: GoogleProject, clusterName: ClusterName) = {
    resourcesApiAsPet(creatorEmail, googleProject).deleteResource(notebookClusterResourceTypeName, getClusterResourceId(googleProject, clusterName))
  }

  def hasActionOnBillingProjectResource(userEmail: WorkbenchEmail, googleProject: GoogleProject, action: String): Boolean = {
    hasActionOnResource(billingProjectResourceTypeName, googleProject.value, userEmail, googleProject, action)
  }

  def hasActionOnNotebookClusterResource(userEmail: WorkbenchEmail, googleProject: GoogleProject, clusterName: ClusterName, action: String): Boolean = {
    hasActionOnResource(notebookClusterResourceTypeName, getClusterResourceId(googleProject, clusterName), userEmail, googleProject, action)
  }

  def getUserProxyFromSam(userEmail: WorkbenchEmail): WorkbenchEmail = {
    val samAPI = samGoogleApi(getAccessTokenUsingPem(leoEmail, leoPem))
    WorkbenchEmail(samAPI.getProxyGroup(userEmail.value))
  }

  def getPetServiceAccount(userEmail: WorkbenchEmail, googleProject: GoogleProject): WorkbenchEmail = {
    WorkbenchEmail(googleApiAsPet(userEmail, googleProject).getPetServiceAccount(googleProject.value))
  }

  private def hasActionOnResource(resourceType: String, resourceName: String, userEmail: WorkbenchEmail, googleProject: GoogleProject, action: String): Boolean = {
    resourcesApiAsPet(userEmail, googleProject).resourceAction(resourceType, resourceName, action)
  }

}
