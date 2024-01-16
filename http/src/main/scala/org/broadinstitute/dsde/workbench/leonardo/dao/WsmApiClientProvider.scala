package org.broadinstitute.dsde.workbench.leonardo.dao

import bio.terra.common.tracing.JerseyTracingFilter
import bio.terra.workspace.api.{ControlledAzureResourceApi, ResourceApi}
import bio.terra.workspace.client.ApiClient
import bio.terra.workspace.model.State
import cats.effect.Async
import cats.mtl.Ask
import cats.syntax.all._
import io.opencensus.trace.Tracing
import org.broadinstitute.dsde.workbench.leonardo.db.WsmResourceType
import org.broadinstitute.dsde.workbench.leonardo.db.WsmResourceType.{AzureDatabase, AzureDisk, AzureVm}
import org.broadinstitute.dsde.workbench.leonardo.{AppContext, WorkspaceId, WsmControlledResourceId, WsmState}
import org.broadinstitute.dsde.workbench.leonardo.util.WithSpanFilter
import org.glassfish.jersey.client.ClientConfig
import org.http4s.Uri
import org.typelevel.log4cats.StructuredLogger

/**
 * Represents a way to get a client for interacting with workspace manager controlled resources.
 * Additional WSM clients can be added here if needed.
 *
 * Based on `org/broadinstitute/dsde/rawls/dataaccess/workspacemanager/WorkspaceManagerApiClientProvider.scala`
 *
 */
trait WsmApiClientProvider[F[_]] {
  def getControlledAzureResourceApi(token: String)(implicit ev: Ask[F, AppContext]): F[ControlledAzureResourceApi]
  def getResourceApi(token: String)(implicit ev: Ask[F, AppContext]): F[ResourceApi]

  def getDiskState(token: String, workspaceId: WorkspaceId, wsmResourceId: WsmControlledResourceId)(implicit
    ev: Ask[F, AppContext]
  ): F[WsmState]

  def getVmState(token: String, workspaceId: WorkspaceId, wsmResourceId: WsmControlledResourceId)(implicit
    ev: Ask[F, AppContext]
  ): F[WsmState]

  def getDatabaseState(token: String, workspaceId: WorkspaceId, wsmResourceId: WsmControlledResourceId)(
    implicit ev: Ask[F, AppContext]
  ): F[WsmState]
}

class HttpWsmClientProvider[F[_]](baseWorkspaceManagerUrl: Uri)(implicit F: Async[F], log: StructuredLogger[F])
    extends WsmApiClientProvider[F] {
  private def getApiClient(token: String)(implicit ev: Ask[F, AppContext]): F[ApiClient] =
    for {
      ctx <- ev.ask
      client = new ApiClient() {
        override def performAdditionalClientConfiguration(clientConfig: ClientConfig): Unit = {
          super.performAdditionalClientConfiguration(clientConfig)
          ctx.span.foreach { span =>
            clientConfig.register(new WithSpanFilter(span))
            clientConfig.register(new JerseyTracingFilter(Tracing.getTracer))
          }
        }
      }
      _ = client.setBasePath(baseWorkspaceManagerUrl.renderString)
      _ = client.setAccessToken(token)
    } yield client
  override def getResourceApi(token: String)(implicit ev: Ask[F, AppContext]): F[ResourceApi] =
    getApiClient(token).map(apiClient => new ResourceApi(apiClient))

  override def getControlledAzureResourceApi(token: String)(implicit
    ev: Ask[F, AppContext]
  ): F[ControlledAzureResourceApi] =
    getApiClient(token).map(apiClient => new ControlledAzureResourceApi(apiClient))

  override def getVmState(token: String, workspaceId: WorkspaceId, wsmResourceId: WsmControlledResourceId)(implicit
    ev: Ask[F, AppContext]
  ): F[WsmState] = for {
    wsmApi <- getControlledAzureResourceApi(token)
    attempt <- F.delay(wsmApi.getAzureVm(workspaceId.value, wsmResourceId.value)).attempt
    state = attempt match {
      case Right(result) => Some(result.getMetadata.getState.getValue)
      case Left(_)       => None
    }
  } yield WsmState(state)

  override def getDiskState(token: String, workspaceId: WorkspaceId, wsmResourceId: WsmControlledResourceId)(implicit
    ev: Ask[F, AppContext]
  ): F[WsmState] = for {
    wsmApi <- getControlledAzureResourceApi(token)
    attempt <- F.delay(wsmApi.getAzureDisk(workspaceId.value, wsmResourceId.value)).attempt
    state = attempt match {
      case Right(result) => Some(result.getMetadata.getState.getValue)
      case Left(_)       => None
    }
  } yield WsmState(state)

  override def getDatabaseState(token: String, workspaceId: WorkspaceId, wsmResourceId: WsmControlledResourceId)(
    implicit ev: Ask[F, AppContext]
  ): F[WsmState] = for {
    wsmApi <- getControlledAzureResourceApi(token)
    attempt <- F.delay(wsmApi.getAzureDatabase(workspaceId.value, wsmResourceId.value)).attempt
    state = attempt match {
      case Right(result) => Some(result.getMetadata.getState.getValue)
      case Left(_)       => None
    }
  } yield WsmState(state)

}
