package org.broadinstitute.dsde.workbench.leonardo.dao

import bio.terra.workspace.api.{ControlledAzureResourceApi, ResourceApi, WorkspaceApi}
import bio.terra.workspace.client.ApiClient
import bio.terra.workspace.model.{IamRole, ResourceMetadata, State}
import cats.effect.Async
import cats.mtl.Ask
import cats.syntax.all._
import org.broadinstitute.dsde.workbench.azure.{AzureCloudContext, ManagedResourceGroupName, SubscriptionId, TenantId}
import org.broadinstitute.dsde.workbench.leonardo.db.WsmResourceType
import org.broadinstitute.dsde.workbench.leonardo.{AppContext, WorkspaceId, WsmControlledResourceId, WsmState}
import org.broadinstitute.dsde.workbench.leonardo.util.WithSpanFilter
import org.broadinstitute.dsde.workbench.model.google.GoogleProject
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

  // WSM state can be BROKEN, CREATING, READY, UPDATING or NONE, (deleted or doesn't exist)
  val possibleStatuses: Array[WsmState] =
    State.values().map(_.toString).map(Some(_)).map(WsmState(_)) :+ WsmState(Some("NONE"))

  def getWorkspace(token: String, workspaceId: WorkspaceId, iamRole: IamRole = IamRole.READER)(implicit
    ev: Ask[F, AppContext]
  ): F[Option[WorkspaceDescription]]

  def getControlledAzureResourceApi(token: String)(implicit ev: Ask[F, AppContext]): F[ControlledAzureResourceApi]
  def getResourceApi(token: String)(implicit ev: Ask[F, AppContext]): F[ResourceApi]

  def getWorkspaceApi(token: String)(implicit ev: Ask[F, AppContext]): F[WorkspaceApi]
  def getDisk(token: String, workspaceId: WorkspaceId, wsmResourceId: WsmControlledResourceId)(implicit
    ev: Ask[F, AppContext],
    log: StructuredLogger[F]
  ): F[Option[ResourceMetadata]]

  def getVm(token: String, workspaceId: WorkspaceId, wsmResourceId: WsmControlledResourceId)(implicit
    ev: Ask[F, AppContext],
    log: StructuredLogger[F]
  ): F[Option[ResourceMetadata]]

  def getDatabase(token: String, workspaceId: WorkspaceId, wsmResourceId: WsmControlledResourceId)(implicit
    ev: Ask[F, AppContext],
    log: StructuredLogger[F]
  ): F[Option[ResourceMetadata]]

  def getNamespace(token: String, workspaceId: WorkspaceId, wsmResourceId: WsmControlledResourceId)(implicit
    ev: Ask[F, AppContext],
    log: StructuredLogger[F]
  ): F[Option[ResourceMetadata]]

  def getIdentity(token: String, workspaceId: WorkspaceId, wsmResourceId: WsmControlledResourceId)(implicit
    ev: Ask[F, AppContext],
    log: StructuredLogger[F]
  ): F[Option[ResourceMetadata]]

  def getWsmState(token: String,
                  workspaceId: WorkspaceId,
                  wsmResourceId: WsmControlledResourceId,
                  resourceType: WsmResourceType
  )(implicit
    ev: Ask[F, AppContext],
    log: StructuredLogger[F]
  ): F[WsmState]
}

class HttpWsmClientProvider[F[_]](baseWorkspaceManagerUrl: Uri)(implicit F: Async[F]) extends WsmApiClientProvider[F] {

  /**
   * This function wraps the wsm generated client getWorkspace
   * The purpose of it is to sanitize the output into the same model our original custom DAO used to reduce the surface area of its removal
   */
  override def getWorkspace(token: String, workspaceId: WorkspaceId, iamRole: IamRole = IamRole.READER)(implicit
    ev: Ask[F, AppContext]
  ): F[Option[WorkspaceDescription]] =
    for {
      workspaceApi <- getWorkspaceApi(token)
      workspaceDescAttempt <- F.delay(workspaceApi.getWorkspace(workspaceId.value, iamRole)).attempt
      workspaceDescUnchecked <- F.fromEither(workspaceDescAttempt)
      workspaceDescOpt = workspaceDescUnchecked match {
        case emptyWorkspace if emptyWorkspace == null => None
        case workspace                                => Some(workspace)
      }
      workspaceDescResult = workspaceDescOpt.map(workspaceDesc =>
        WorkspaceDescription(
          workspaceId,
          workspaceDesc.getDisplayName,
          workspaceDesc.getSpendProfile,
          workspaceDesc.getAzureContext match {
            case emptyContext if emptyContext == null => None
            case context =>
              Some(
                AzureCloudContext(TenantId(context.getTenantId),
                                  SubscriptionId(context.getSubscriptionId),
                                  ManagedResourceGroupName(context.getResourceGroupId)
                )
              )
          },
          workspaceDesc.getGcpContext match {
            case emptyContext if emptyContext == null => None
            case context                              => Some(GoogleProject(context.getProjectId))
          }
        )
      )
    } yield workspaceDescResult

  private def getApiClient(token: String)(implicit ev: Ask[F, AppContext]): F[ApiClient] =
    for {
      ctx <- ev.ask
      client = new ApiClient() {
        override def performAdditionalClientConfiguration(clientConfig: ClientConfig): Unit = {
          super.performAdditionalClientConfiguration(clientConfig)
          ctx.span.foreach { span =>
            clientConfig.register(new WithSpanFilter(span))
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

  override def getWorkspaceApi(token: String)(implicit ev: Ask[F, AppContext]): F[WorkspaceApi] =
    getApiClient(token).map(apiClient => new WorkspaceApi(apiClient))

  override def getVm(token: String, workspaceId: WorkspaceId, wsmResourceId: WsmControlledResourceId)(implicit
    ev: Ask[F, AppContext],
    log: StructuredLogger[F]
  ): F[Option[ResourceMetadata]] = for {
    wsmApi <- getControlledAzureResourceApi(token)
    attempt <- F.delay(wsmApi.getAzureVm(workspaceId.value, wsmResourceId.value)).attempt
    vm = attempt match {
      case Right(result) => Some(result.getMetadata)
      case Left(_)       => None
    }
  } yield vm

  override def getDisk(token: String, workspaceId: WorkspaceId, wsmResourceId: WsmControlledResourceId)(implicit
    ev: Ask[F, AppContext],
    log: StructuredLogger[F]
  ): F[Option[ResourceMetadata]] = for {
    wsmApi <- getControlledAzureResourceApi(token)
    attempt <- F.delay(wsmApi.getAzureDisk(workspaceId.value, wsmResourceId.value)).attempt
    disk = attempt match {
      case Right(result) => Some(result.getMetadata)
      case Left(_)       => None
    }
  } yield disk

  override def getDatabase(token: String, workspaceId: WorkspaceId, wsmResourceId: WsmControlledResourceId)(implicit
    ev: Ask[F, AppContext],
    log: StructuredLogger[F]
  ): F[Option[ResourceMetadata]] = for {
    wsmApi <- getControlledAzureResourceApi(token)
    attempt <- F.delay(wsmApi.getAzureDatabase(workspaceId.value, wsmResourceId.value)).attempt
    db = attempt match {
      case Right(result) => Some(result.getMetadata)
      case Left(_)       => None
    }
  } yield db

  override def getNamespace(token: String, workspaceId: WorkspaceId, wsmResourceId: WsmControlledResourceId)(implicit
    ev: Ask[F, AppContext],
    log: StructuredLogger[F]
  ): F[Option[ResourceMetadata]] = for {
    wsmApi <- getControlledAzureResourceApi(token)
    attempt <- F.delay(wsmApi.getAzureKubernetesNamespace(workspaceId.value, wsmResourceId.value)).attempt
    namespace = attempt match {
      case Right(result) => Some(result.getMetadata)
      case Left(_)       => None
    }
  } yield namespace

  override def getIdentity(token: String, workspaceId: WorkspaceId, wsmResourceId: WsmControlledResourceId)(implicit
    ev: Ask[F, AppContext],
    log: StructuredLogger[F]
  ): F[Option[ResourceMetadata]] = for {
    wsmApi <- getControlledAzureResourceApi(token)
    attempt <- F.delay(wsmApi.getAzureManagedIdentity(workspaceId.value, wsmResourceId.value)).attempt
    id = attempt match {
      case Right(result) => Some(result.getMetadata)
      case Left(_)       => None
    }
  } yield id

  override def getWsmState(token: String,
                           workspaceId: WorkspaceId,
                           wsmResourceId: WsmControlledResourceId,
                           resourceType: WsmResourceType
  )(implicit
    ev: Ask[F, AppContext],
    log: StructuredLogger[F]
  ): F[WsmState] = for {
    resource <- resourceType match {
      case WsmResourceType.AzureVm =>
        getVm(token, workspaceId, wsmResourceId)
      case WsmResourceType.AzureDatabase =>
        getDatabase(token, workspaceId, wsmResourceId)
      case WsmResourceType.AzureKubernetesNamespace =>
        getNamespace(token, workspaceId, wsmResourceId)
      case WsmResourceType.AzureManagedIdentity =>
        getIdentity(token, workspaceId, wsmResourceId)
      case WsmResourceType.AzureDisk =>
        getDisk(token, workspaceId, wsmResourceId)
      case WsmResourceType.AzureStorageContainer =>
        F.pure(None) // TODO: no get endpoint for a storage container in WSM yet
    }
    state = resource match {
      case Some(rs) => Some(rs.getState.getValue)
      case None     => None
    }
  } yield toWsmStatus(state)

}
