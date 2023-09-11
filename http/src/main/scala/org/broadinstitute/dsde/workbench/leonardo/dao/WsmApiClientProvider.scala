package org.broadinstitute.dsde.workbench.leonardo.dao

import bio.terra.workspace.api.ControlledAzureResourceApi
import bio.terra.workspace.client.ApiClient
import cats.effect.Async
import cats.mtl.Ask
import cats.syntax.all._
import io.opencensus.trace.Tracing
import org.broadinstitute.dsde.workbench.leonardo.AppContext
import org.glassfish.jersey.client.ClientConfig
import org.http4s.Uri

/**
 * Represents a way to get a client for interacting with workspace manager controlled resources.
 * Additional WSM clients can be added here if needed.
 *
 * Based on `org/broadinstitute/dsde/rawls/dataaccess/workspacemanager/WorkspaceManagerApiClientProvider.scala`
 *
 */
trait WsmApiClientProvider[F[_]] {
  def getControlledAzureResourceApi(token: String)(implicit ev: Ask[F, AppContext]): F[ControlledAzureResourceApi]
}

class HttpWsmClientProvider[F[_]](baseWorkspaceManagerUrl: Uri)(implicit F: Async[F]) extends WsmApiClientProvider[F] {
  private def getApiClient(token: String)(implicit ev: Ask[F, AppContext]): F[ApiClient] =
    for {
      ctx <- ev.ask
      client = new ApiClient() {
        override def performAdditionalClientConfiguration(clientConfig: ClientConfig): Unit = {
          super.performAdditionalClientConfiguration(clientConfig)
          ctx.span.foreach { span =>
            clientConfig.register(Tracing.getTracer.withSpan(span))
          }
        }
      }
      _ = client.setBasePath(baseWorkspaceManagerUrl.renderString)
      _ = client.setAccessToken(token)
    } yield client

  def getControlledAzureResourceApi(token: String)(implicit ev: Ask[F, AppContext]): F[ControlledAzureResourceApi] =
    getApiClient(token).map(apiClient => new ControlledAzureResourceApi(apiClient))
}
