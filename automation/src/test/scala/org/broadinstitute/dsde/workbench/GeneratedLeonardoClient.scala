package org.broadinstitute.dsde.workbench

import cats.effect.IO
import org.broadinstitute.dsde.workbench.auth.AuthToken
import org.broadinstitute.dsde.workbench.client.leonardo.api.{DisksApi, RuntimesApi, ProxyApi}
import org.broadinstitute.dsde.workbench.client.leonardo.ApiClient
import org.broadinstitute.dsde.workbench.client.leonardo.model.{GetPersistentDiskV2Response, GetRuntimeResponse, DiskStatus, ClusterStatus}
import org.broadinstitute.dsde.workbench.leonardo.LeonardoConfig
import org.http4s.Uri

/**
 * This is the wrapper for the generated Leonardo API client
 * Favor this client over `LeonardoApiClient`
 */
object GeneratedLeonardoClient {
  private val rootUri = Uri.fromString(LeonardoConfig.Leonardo.apiUrl)

  def runtimeInStateOrError(status: ClusterStatus): DoneCheckable[GetRuntimeResponse] =
    (op: GetRuntimeResponse) => op.getStatus.equals(status) || op.getStatus.equals(ClusterStatus.ERROR)

  def diskInStateOrError(status: DiskStatus): DoneCheckable[GetPersistentDiskV2Response] =
    (op: GetPersistentDiskV2Response) => op.getStatus.equals(status) || op.getStatus.equals(DiskStatus.FAILED)

  def generateRuntimesApi()(implicit accessToken: IO[AuthToken]): IO[RuntimesApi] =
    for {
      apiClient <- getClient()
      api <- IO(new RuntimesApi(apiClient))
    } yield api

  def generateDisksApi()(implicit accessToken: IO[AuthToken]): IO[DisksApi] =
    for {
      apiClient <- getClient()
      api <- IO(new DisksApi(apiClient))
    } yield api

  def generateProxyApi(runtime: GetRuntimeResponse)(implicit accessToken: IO[AuthToken]): IO[ApiClient] =
    for {
      apiClient <- getClient()
      client <- IO(apiClient.setBasePath(s"${runtime.getProxyUrl}/lab"))
//      x: Call = new Call
//      _ <- client.execute(Call())
    } yield client

  private def getClient()(implicit accessToken: IO[AuthToken]): IO[ApiClient] =
    for {
      apiClient <- IO(new ApiClient())
      token <- accessToken
      uri <- IO.fromEither(rootUri)
      _ <- IO(apiClient.setBasePath(uri.toString()))
      _ <- IO(apiClient.setAccessToken(token.value))
    } yield apiClient
}
