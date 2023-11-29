package org.broadinstitute.dsde.workbench

import cats.effect.{IO, Resource}
import org.broadinstitute.dsde.workbench.auth.AuthToken
import org.broadinstitute.dsde.workbench.client.leonardo.api.{DisksApi, RuntimesApi}
import org.broadinstitute.dsde.workbench.client.leonardo.ApiClient
import org.broadinstitute.dsde.workbench.client.leonardo.model.{
  ClusterStatus,
  DiskStatus,
  GetPersistentDiskV2Response,
  GetRuntimeResponse
}
import org.broadinstitute.dsde.workbench.leonardo.{LeonardoApiClient, LeonardoConfig}
import org.broadinstitute.dsde.workbench.util2.ExecutionContexts
import org.http4s.{Headers, Request, Uri}
import org.http4s._
import org.http4s.client.Client
import org.http4s.client.middleware.{Logger, Retry, RetryPolicy}
import org.http4s.headers.Authorization
import org.http4s.blaze.client.BlazeClientBuilder

import scala.concurrent.duration._

/**
 * This is the wrapper for the generated Leonardo API client
 * Favor this client over `LeonardoApiClient`
 */
object GeneratedLeonardoClient {
  private val rootUri = Uri.fromString(LeonardoConfig.Leonardo.apiUrl)

  val client: Resource[IO, Client[IO]] =
    for {
      blockingEc <- ExecutionContexts.cachedThreadPool[IO]
      retryPolicy = RetryPolicy[IO](RetryPolicy.exponentialBackoff(30 seconds, 5))
      client <- BlazeClientBuilder[IO](blockingEc).resource.map(c => Retry(retryPolicy)(c))
    } yield Logger[IO](logHeaders = true, logBody = true)(client)

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

  def generateAppsApi()(implicit accessToken: IO[AuthToken]): IO[AppsApi] =
    for {
      apiClient <- getClient()
      api <- IO(new AppsApi(apiClient))
    } yield api


  def testProxyUrl(
    runtime: org.broadinstitute.dsde.workbench.client.leonardo.model.GetRuntimeResponse
  )(implicit client: Client[IO], authorization: IO[Authorization]): IO[Unit] =
    for {
      authHeader <- authorization
      r <- client
        .run(
          Request[IO](
            method = Method.GET,
            headers = Headers(authHeader),
            uri = Uri
              .unsafeFromString(runtime.getProxyUrl.concat("/lab"))
          )
        )
        .use { resp =>
          if (!resp.status.isSuccess) {
            LeonardoApiClient
              .onError(s"Failed to hit jupyterlab proxy Url")(resp)
              .flatMap(IO.raiseError)
          } else
            IO.unit
        }
    } yield r

  private def getClient()(implicit accessToken: IO[AuthToken]): IO[ApiClient] =
    for {
      apiClient <- IO(new ApiClient())
      token <- accessToken
      uri <- IO.fromEither(rootUri)
      _ <- IO(apiClient.setBasePath(uri.toString()))
      _ <- IO(apiClient.setAccessToken(token.value))
    } yield apiClient
      .setWriteTimeout(LeonardoConfig.LeonardoClient.writeTimeout)
      .setReadTimeout(LeonardoConfig.LeonardoClient.readTimeout)
      .setConnectTimeout(LeonardoConfig.LeonardoClient.connectionTimeout)
}
