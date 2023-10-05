package org.broadinstitute.dsde.workbench.leonardo
package dao

import org.broadinstitute.dsde.workbench.client.sam.ApiClient
import org.broadinstitute.dsde.workbench.client.sam.api.ResourcesApi
import org.broadinstitute.dsde.workbench.client.sam.api.ConfigApi
import org.broadinstitute.dsde.workbench.client.sam.model.{ResourceType, UserResourcesResponse}

// Implicit for .asScala
import scala.jdk.CollectionConverters.ListHasAsScala

class SamClient(config: HttpSamDaoConfig) {
  private def getResourcesApi(accessToken: String): ResourcesApi = {
    val apiClient = new ApiClient()
    apiClient.setAccessToken(accessToken)
    apiClient.setBasePath(config.samUri.toString)
    new ResourcesApi(apiClient)
  }

  private def getConfigApi(accessToken: String): ConfigApi = {
    val apiClient = new ApiClient()
    apiClient.setAccessToken(accessToken)
    apiClient.setBasePath(config.samUri.toString)
    new ConfigApi(apiClient)
  }

  /** Can the user perform the given action on the given resource? */
  def hasResourceAction(token: String,
                        resourceType: String,
                        resourceId: String,
                        action: String,
                        userEmail: String
  ): Boolean = {
    val samResourceApi = getResourcesApi(token)
    samResourceApi.resourceActionV2(resourceType, resourceId, action, userEmail)
  }

  /** List all policies granted to the caller on resources of the given type. */
  def listResourcesAndPolicies(token: String, resourceType: String): List[UserResourcesResponse] = {
    val samResourceApi = getResourcesApi(token)
    samResourceApi.listResourcesAndPoliciesV2(resourceType).asScala.toList
  }

  /** List the configuration for all resource types. */
  def listResourceTypes(token: String): List[ResourceType] = {
    val samConfigApi = getConfigApi(token)
    samConfigApi.listResourceTypes.asScala.toList
  }
}
