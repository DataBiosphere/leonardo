package org.broadinstitute.dsde.workbench.leonardo.dao

import bio.terra.workspace.api.{ControlledAzureResourceApi, ResourceApi}
import bio.terra.workspace.client.ApiClient
import org.http4s.Uri

/**
 * Represents a way to get a client for interacting with workspace manager controlled resources.
 * Additional WSM clients can be added here if needed.
 *
 * Based on `org/broadinstitute/dsde/rawls/dataaccess/workspacemanager/WorkspaceManagerApiClientProvider.scala`
 *
 */
trait WsmApiClientProvider {
  def getControlledAzureResourceApi(token: String): ControlledAzureResourceApi
  def getResourceApi(token: String): ResourceApi
}

class HttpWsmClientProvider(baseWorkspaceManagerUrl: Uri) extends WsmApiClientProvider {
  private def getApiClient: ApiClient = {
    val client: ApiClient = new ApiClient()
    client.setBasePath(baseWorkspaceManagerUrl.renderString)
    client
  }

  def getControlledAzureResourceApi(token: String): ControlledAzureResourceApi = {
    val apiClient = getApiClient
    apiClient.setAccessToken(token)
    new ControlledAzureResourceApi(apiClient)
  }

  def getResourceApi(token: String): ResourceApi = {
    val apiClient = getApiClient
    apiClient.setAccessToken(token)
    new ResourceApi(apiClient)
  }
}
