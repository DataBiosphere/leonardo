package org.broadinstitute.dsde.workbench.leonardo.auth.sam

import java.io.{ByteArrayInputStream, File}
import java.util.concurrent.TimeUnit

import com.google.api.client.googleapis.auth.oauth2.GoogleCredential
import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport
import com.google.api.client.json.jackson2.JacksonFactory
import com.google.api.services.plus.PlusScopes
import com.google.api.services.storage.StorageScopes
import com.google.auth.oauth2.ServiceAccountCredentials
import com.google.common.cache.{CacheBuilder, CacheLoader}
import com.typesafe.scalalogging.LazyLogging
import io.swagger.client.ApiClient
import io.swagger.client.api.{GoogleApi, ResourcesApi}
import org.broadinstitute.dsde.workbench.leonardo.model.google.ClusterName
import org.broadinstitute.dsde.workbench.model.{UserInfo, WorkbenchEmail}
import org.broadinstitute.dsde.workbench.model.google.GoogleProject

import scala.collection.JavaConverters._
import scala.concurrent.duration.FiniteDuration

case class UserEmailAndProject(userEmail: WorkbenchEmail, googleProject: GoogleProject)

class SwaggerSamClient(samBasePath: String, cacheEnabled: Boolean, cacheExpiryTime: FiniteDuration, cacheMaxSize: Int, leoEmail: WorkbenchEmail, leoPem: File) extends LazyLogging {

  private val httpTransport = GoogleNetHttpTransport.newTrustedTransport
  private val jsonFactory = JacksonFactory.getDefaultInstance
  private val saScopes = Seq(PlusScopes.USERINFO_EMAIL, PlusScopes.USERINFO_PROFILE, StorageScopes.DEVSTORAGE_READ_ONLY)

  private val notebookClusterResourceTypeName = "notebook-cluster"
  private val billingProjectResourceTypeName = "billing-project"

  private val projectOwnerPolicyName = "owner"
  private val clusterCreatorPolicyName = "creator"


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
  private[auth] def samResourcesApiAsPet(userEmail: WorkbenchEmail, googleProject: GoogleProject): ResourcesApi = {
    samResourcesApi(getCachedPetAccessToken(userEmail, googleProject))
  }

  //A google API as the given user's pet SA
  private[auth] def samGoogleApiAsPet(userEmail: WorkbenchEmail, googleProject: GoogleProject): GoogleApi = {
    samGoogleApi(getCachedPetAccessToken(userEmail, googleProject))
  }

  //"Fast" lookup of pet's access token, using the cache.
  def getCachedPetAccessToken(userEmail: WorkbenchEmail, googleProject: GoogleProject): String = {
    if (cacheEnabled) {
      petTokenCache.get(UserEmailAndProject(userEmail, googleProject))
    } else {
      getPetAccessTokenFromSam(userEmail, googleProject)
    }
  }

  def invalidatePetAccessToken(userEmail: WorkbenchEmail, googleProject: GoogleProject): Unit = {
    petTokenCache.invalidate(UserEmailAndProject(userEmail, googleProject))
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
    logger.debug(s"Calling Sam getPetAccessTokenFromSam as user ${userEmail.value} and project ${googleProject.value}")
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

  def createNotebookClusterResource(internalId: String, userEmail: WorkbenchEmail, googleProject: GoogleProject, clusterName: ClusterName) = {
    logger.debug(s"Calling Sam createNotebookClusterResource as PET for user [${userEmail.value}] with project [${googleProject.value}] and clusterName [${clusterName.value}]")
    // this is called as the user's pet
    val samAPI = samResourcesApiAsPet(userEmail, googleProject)
    samAPI.createResource(notebookClusterResourceTypeName, internalId)
  }

  def deleteNotebookClusterResource(internalId: String, creatorEmail: WorkbenchEmail, googleProject: GoogleProject, clusterName: ClusterName) = {
    logger.debug(s"Calling Sam deleteNotebookClusterResource as PET for user [${creatorEmail.value}] with project [${googleProject.value}] and clusterName [${clusterName.value}]")
    // this is called as the user's pet
    val samAPI = samResourcesApiAsPet(creatorEmail, googleProject)
    samAPI.deleteResource(notebookClusterResourceTypeName, internalId)
  }

  def hasActionOnBillingProjectResource(userInfo: UserInfo, googleProject: GoogleProject, action: String): Boolean = {
    // this is called with the user's token
    hasActionOnResource(userInfo, billingProjectResourceTypeName, googleProject.value, action)
  }

  def hasActionOnNotebookClusterResource(internalId: String, userInfo: UserInfo, action: String): Boolean = {
    // this is called with the user's token
    hasActionOnResource(userInfo, notebookClusterResourceTypeName, internalId, action)
  }

  def getUserProxyFromSam(userEmail: WorkbenchEmail): WorkbenchEmail = {
    logger.debug(s"Calling Sam getUserProxyFromSam as user ${leoEmail} with email ${userEmail.value}")
    // this is called as the Leo service account
    val samAPI = samGoogleApi(getAccessTokenUsingPem(leoEmail, leoPem))
    WorkbenchEmail(samAPI.getProxyGroup(userEmail.value))
  }

  def getPetServiceAccount(userInfo: UserInfo, googleProject: GoogleProject): WorkbenchEmail = {
    logger.debug(s"Calling Sam getPetServiceAccount as user [${userInfo}] and project [${googleProject.value}]")
    // this is called with the user's token
    val samAPI = samGoogleApi(userInfo.accessToken.token)
    WorkbenchEmail(samAPI.getPetServiceAccount(googleProject.value))
  }

  def listOwningProjects(userInfo: UserInfo): List[GoogleProject] = {
    logger.debug(s"Calling Sam listResourcesAndPolicies as user [${userInfo}] and resource type [${billingProjectResourceTypeName}]")
    // this is called with the user's token
    val samAPI = samResourcesApi(userInfo.accessToken.token)
    samAPI.listResourcesAndPolicies(billingProjectResourceTypeName).asScala.toList
      .filter(_.getAccessPolicyName == projectOwnerPolicyName)
      .map(p => GoogleProject(p.getResourceId))
  }

  def listCreatedClusters(userInfo: UserInfo): List[(GoogleProject, ClusterName)] = {
    logger.debug(s"Calling Sam listResourcesAndPolicies as user [${userInfo}] and resource type [${notebookClusterResourceTypeName}]")
    // this is called with the user's token
    val samAPI = samResourcesApi(userInfo.accessToken.token)
    samAPI.listResourcesAndPolicies(notebookClusterResourceTypeName).asScala.toList
      .filter(_.getAccessPolicyName == clusterCreatorPolicyName)
      .flatMap(p => parseClusterResourceId(p.getResourceId))
  }

  private def hasActionOnResource(userInfo: UserInfo, resourceType: String, resourceName: String, action: String): Boolean = {
    logger.debug(s"Calling Sam resources API as user [${userInfo}] for resource type [$resourceType], resource name [$resourceName] and action [$action]")
    // this is called with the user's token
    val samAPI = samResourcesApi(userInfo.accessToken.token)
    samAPI.resourceAction(resourceType, resourceName, action)
  }

  private def parseClusterResourceId(id: String): Option[(GoogleProject, ClusterName)] = {
    val splitId = id.split("_")
    if (splitId.length == 2) {
      Some(GoogleProject(splitId(0)), ClusterName(splitId(1)))
    } else None
  }
}
