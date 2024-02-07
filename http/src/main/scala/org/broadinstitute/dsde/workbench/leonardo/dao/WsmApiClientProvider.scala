package org.broadinstitute.dsde.workbench.leonardo.dao

import bio.terra.common.tracing.JerseyTracingFilter
import bio.terra.workspace.api.{ControlledAzureResourceApi, ResourceApi}
import bio.terra.workspace.client.ApiClient
import bio.terra.workspace.model.State
import cats.effect.Async
import cats.mtl.Ask
import cats.syntax.all._
import io.opencensus.trace.Tracing
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

  val possibleStatuses: Array[WsmState] =
    State.values().map(_.toString).map(Some(_)).map(WsmState(_)) :+ WsmState(Some("DELETED"))

  def getControlledAzureResourceApi(token: String)(implicit ev: Ask[F, AppContext]): F[ControlledAzureResourceApi]
  def getResourceApi(token: String)(implicit ev: Ask[F, AppContext]): F[ResourceApi]
  def getDiskState(token: String, workspaceId: WorkspaceId, wsmResourceId: WsmControlledResourceId)(implicit
    ev: Ask[F, AppContext],
    log: StructuredLogger[F]
  ): F[WsmState]

  def getVmState(token: String, workspaceId: WorkspaceId, wsmResourceId: WsmControlledResourceId)(implicit
    ev: Ask[F, AppContext],
    log: StructuredLogger[F]
  ): F[WsmState]

  def getDatabaseState(token: String, workspaceId: WorkspaceId, wsmResourceId: WsmControlledResourceId)(implicit
    ev: Ask[F, AppContext],
    log: StructuredLogger[F]
  ): F[WsmState]

  def getNamespaceState(token: String, workspaceId: WorkspaceId, wsmResourceId: WsmControlledResourceId)(implicit
    ev: Ask[F, AppContext],
    log: StructuredLogger[F]
  ): F[WsmState]

  def getIdentityState(token: String, workspaceId: WorkspaceId, wsmResourceId: WsmControlledResourceId)(implicit
    ev: Ask[F, AppContext],
    log: StructuredLogger[F]
  ): F[WsmState]
}

class HttpWsmClientProvider[F[_]](baseWorkspaceManagerUrl: Uri)(implicit F: Async[F]) extends WsmApiClientProvider[F] {
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

  private def toWsmStatus(
    state: Option[String]
  )(implicit logger: StructuredLogger[F]): WsmState = {
    val wsmState = WsmState(state)
    if (!possibleStatuses.contains(wsmState)) logger.warn("Invalid Wsm status")
    wsmState
  }
  override def getResourceApi(token: String)(implicit ev: Ask[F, AppContext]): F[ResourceApi] =
    getApiClient(token).map(apiClient => new ResourceApi(apiClient))

  override def getControlledAzureResourceApi(token: String)(implicit
    ev: Ask[F, AppContext]
  ): F[ControlledAzureResourceApi] =
    getApiClient(token).map(apiClient => new ControlledAzureResourceApi(apiClient))

  override def getVmState(token: String, workspaceId: WorkspaceId, wsmResourceId: WsmControlledResourceId)(implicit
    ev: Ask[F, AppContext],
    log: StructuredLogger[F]
  ): F[WsmState] = for {
    wsmApi <- getControlledAzureResourceApi(token)
    attempt <- F.delay(wsmApi.getAzureVm(workspaceId.value, wsmResourceId.value)).attempt
    state = attempt match {
      case Right(result) => Some(result.getMetadata.getState.getValue)
      case Left(_)       => None
    }
  } yield toWsmStatus(state)

  override def getDiskState(token: String, workspaceId: WorkspaceId, wsmResourceId: WsmControlledResourceId)(implicit
    ev: Ask[F, AppContext],
    log: StructuredLogger[F]
  ): F[WsmState] = for {
    wsmApi <- getControlledAzureResourceApi(token)
    attempt <- F.delay(wsmApi.getAzureDisk(workspaceId.value, wsmResourceId.value)).attempt
    state = attempt match {
      case Right(result) => Some(result.getMetadata.getState.getValue)
      case Left(_)       => None
    }
  } yield toWsmStatus(state)

  override def getDatabaseState(token: String, workspaceId: WorkspaceId, wsmResourceId: WsmControlledResourceId)(
    implicit
    ev: Ask[F, AppContext],
    log: StructuredLogger[F]
  ): F[WsmState] = for {
    wsmApi <- getControlledAzureResourceApi(token)
    attempt <- F.delay(wsmApi.getAzureDatabase(workspaceId.value, wsmResourceId.value)).attempt
    state = attempt match {
      case Right(result) => Some(result.getMetadata.getState.getValue)
      case Left(_)       => None
    }
  } yield toWsmStatus(state)

  override def getNamespaceState(token: String, workspaceId: WorkspaceId, wsmResourceId: WsmControlledResourceId)(
    implicit
    ev: Ask[F, AppContext],
    log: StructuredLogger[F]
  ): F[WsmState] = for {
    wsmApi <- getControlledAzureResourceApi(token)
    attempt <- F.delay(wsmApi.getAzureKubernetesNamespace(workspaceId.value, wsmResourceId.value)).attempt
    state = attempt match {
      case Right(result) => Some(result.getMetadata.getState.getValue)
      case Left(_)       => None
    }
  } yield toWsmStatus(state)

  override def getIdentityState(token: String, workspaceId: WorkspaceId, wsmResourceId: WsmControlledResourceId)(
    implicit
    ev: Ask[F, AppContext],
    log: StructuredLogger[F]
  ): F[WsmState] = for {
    wsmApi <- getControlledAzureResourceApi(token)
    attempt <- F.delay(wsmApi.getAzureManagedIdentity(workspaceId.value, wsmResourceId.value)).attempt
    state = attempt match {
      case Right(result) => Some(result.getMetadata.getState.getValue)
      case Left(_)       => None
    }
  } yield toWsmStatus(state)

}
