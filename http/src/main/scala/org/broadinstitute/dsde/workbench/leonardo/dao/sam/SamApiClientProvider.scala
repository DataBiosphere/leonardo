package org.broadinstitute.dsde.workbench.leonardo.dao.sam

import cats.effect.Async
import cats.mtl.Ask
import cats.syntax.all._
import okhttp3.Protocol
import org.broadinstitute.dsde.workbench.client.sam.ApiClient
import org.broadinstitute.dsde.workbench.client.sam.api.{AzureApi, GoogleApi, ResourcesApi}
import org.broadinstitute.dsde.workbench.leonardo.AppContext

import scala.concurrent.duration._
import scala.jdk.CollectionConverters._
import scala.jdk.DurationConverters._

/**
 * Provides access to various Sam clients:
 * - ResourcesApi is used for interacting with Sam resources and policies to enforce access control.
 * - GoogleApi is used for Google-specific extensions for users, such as pet service accounts and proxy groups.
 * - AzureApi is used for Azure-specific extensions for users, such as pet managed identities.
 */
trait SamApiClientProvider[F[_]] {
  def resourcesApi(token: String)(implicit ev: Ask[F, AppContext]): F[ResourcesApi]
  def googleApi(token: String)(implicit ev: Ask[F, AppContext]): F[GoogleApi]
  def azureApi(token: String)(implicit ev: Ask[F, AppContext]): F[AzureApi]
}

class HttpSamApiClientProvider[F[_]](samUrl: String)(implicit F: Async[F]) extends SamApiClientProvider[F] {
  private val okHttpClient = new ApiClient().getHttpClient
  private val timeout = 30 seconds

  private def getApiClient(token: String)(implicit ev: Ask[F, AppContext]): F[ApiClient] =
    for {
      ctx <- ev.ask
      okHttpClientBuilder = okHttpClient.newBuilder
        .readTimeout(timeout.toJava)
        .protocols(Seq(Protocol.HTTP_1_1).asJava)
      // TODO add otel interceptors
      //  See https://broadworkbench.atlassian.net/browse/IA-5052
      apiClient = new ApiClient(okHttpClientBuilder.build()).setBasePath(samUrl)
      _ = apiClient.setAccessToken(token)
    } yield apiClient

  override def resourcesApi(token: String)(implicit ev: Ask[F, AppContext]): F[ResourcesApi] =
    getApiClient(token).map(api => new ResourcesApi(api))

  override def googleApi(token: String)(implicit ev: Ask[F, AppContext]): F[GoogleApi] =
    getApiClient(token).map(api => new GoogleApi(api))

  override def azureApi(token: String)(implicit ev: Ask[F, AppContext]): F[AzureApi] =
    getApiClient(token).map(api => new AzureApi(api))
}
