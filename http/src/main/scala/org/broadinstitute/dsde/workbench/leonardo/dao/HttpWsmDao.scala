package org.broadinstitute.dsde.workbench.leonardo
package dao

import cats.effect.Async
import cats.implicits._
import cats.mtl.Ask
import org.broadinstitute.dsde.workbench.azure.{
  AKSClusterName,
  ApplicationInsightsName,
  BatchAccountName,
  RelayNamespace
}
import org.broadinstitute.dsde.workbench.google2.{NetworkName, SubnetworkName}
import org.broadinstitute.dsde.workbench.leonardo.config.HttpWsmDaoConfig
import org.broadinstitute.dsde.workbench.leonardo.dao.LandingZoneResourcePurpose.{
  AKS_NODE_POOL_SUBNET,
  LandingZoneResourcePurpose,
  SHARED_RESOURCE,
  WORKSPACE_BATCH_SUBNET
}
import org.broadinstitute.dsde.workbench.leonardo.dao.WsmDecoders._
import org.broadinstitute.dsde.workbench.leonardo.dao.WsmEncoders._
import org.broadinstitute.dsde.workbench.leonardo.db.WsmResourceType
import org.broadinstitute.dsde.workbench.leonardo.util.AppCreationException
import org.broadinstitute.dsde.workbench.model.TraceId
import org.broadinstitute.dsde.workbench.openTelemetry.OpenTelemetryMetrics
import org.http4s._
import org.http4s.circe.CirceEntityDecoder._
import org.http4s.circe.CirceEntityEncoder._
import org.http4s.client.Client
import org.http4s.client.dsl.Http4sClientDsl
import org.http4s.headers.{`Content-Type`, Authorization}
import org.typelevel.ci.CIString
import org.typelevel.log4cats.StructuredLogger

import java.util.UUID

class HttpWsmDao[F[_]](httpClient: Client[F], config: HttpWsmDaoConfig)(implicit
  logger: StructuredLogger[F],
  F: Async[F],
  metrics: OpenTelemetryMetrics[F]
) extends WsmDao[F]
    with Http4sClientDsl[F] {

  val defaultMediaType = `Content-Type`(MediaType.application.json)

  override def createDisk(request: CreateDiskRequest, authorization: Authorization)(implicit
    ev: Ask[F, AppContext]
  ): F[CreateDiskResponse] =
    for {
      ctx <- ev.ask
      res <- httpClient.expectOr[CreateDiskResponse](
        Request[F](
          method = Method.POST,
          uri = config.uri
            .withPath(
              Uri.Path
                .unsafeFromString(
                  s"/api/workspaces/v1/${request.workspaceId.value.toString}/resources/controlled/azure/disks"
                )
            ),
          entity = request,
          headers = headers(authorization, ctx.traceId, true)
        )
      )(onError)
    } yield res

  override def createVm(request: CreateVmRequest, authorization: Authorization)(implicit
    ev: Ask[F, AppContext]
  ): F[CreateVmResult] =
    for {
      ctx <- ev.ask
      res <- httpClient.expectOr[CreateVmResult](
        Request[F](
          method = Method.POST,
          uri = config.uri
            .withPath(
              Uri.Path
                .unsafeFromString(
                  s"/api/workspaces/v1/${request.workspaceId.value.toString}/resources/controlled/azure/vm"
                )
            ),
          entity = request,
          headers = headers(authorization, ctx.traceId, true)
        )
      )(onError)
    } yield res

  def createStorageContainer(request: CreateStorageContainerRequest, authorization: Authorization)(implicit
    ev: Ask[F, AppContext]
  ): F[CreateStorageContainerResult] = for {
    ctx <- ev.ask
    res <- httpClient.expectOr[CreateStorageContainerResult](
      Request[F](
        method = Method.POST,
        uri = config.uri
          .withPath(
            Uri.Path
              .unsafeFromString(
                s"/api/workspaces/v1/${request.workspaceId.value.toString}/resources/controlled/azure/storageContainer"
              )
          ),
        entity = request,
        headers = headers(authorization, ctx.traceId, true)
      )
    )(onError)
  } yield res

  override def getWorkspace(workspaceId: WorkspaceId, authorization: Authorization)(implicit
    ev: Ask[F, AppContext]
  ): F[Option[WorkspaceDescription]] =
    for {
      ctx <- ev.ask
      res <- httpClient.expectOptionOr[WorkspaceDescription](
        Request[F](
          method = Method.GET,
          uri = config.uri
            .withPath(
              Uri.Path
                .unsafeFromString(s"/api/workspaces/v1/${workspaceId.value.toString}")
            ),
          headers = headers(authorization, ctx.traceId, false)
        )
      )(onError)
    } yield res

  override def getLandingZoneResources(billingProfileId: String, userToken: Authorization)(implicit
    ev: Ask[F, AppContext]
  ): F[LandingZoneResources] =
    for {
      // Step 1: call LZ for LZ id
      landingZoneOpt <- getLandingZone(billingProfileId, userToken)
      landingZone <- F.fromOption(
        landingZoneOpt,
        AppCreationException(s"Landing zone not found for billing profile ${billingProfileId}")
      )
      landingZoneId = landingZone.landingZoneId

      // Step 2: call LZ for LZ resources
      lzResourcesByPurpose <- listLandingZoneResourcesByType(landingZoneId, userToken)
      region <- lzResourcesByPurpose
        .flatMap(_.deployedResources)
        .headOption match { // All LZ resources live in a same region. Hence we can grab any resource and find out the region
        case Some(lzResource) =>
          F.pure(
            com.azure.core.management.Region
              .fromName(lzResource.region)
          )
        case None =>
          F.raiseError(new Exception(s"This should never happen. No resource found for LZ(${landingZoneId})"))
      }
      groupedLzResources = lzResourcesByPurpose.foldMap(a =>
        a.deployedResources.groupBy(b => (a.purpose, b.resourceType.toLowerCase))
      )

      aksClusterName <- getLandingZoneResourceName(groupedLzResources,
                                                   "Microsoft.ContainerService/managedClusters",
                                                   SHARED_RESOURCE,
                                                   false
      )
      batchAccountName <- getLandingZoneResourceName(groupedLzResources,
                                                     "Microsoft.Batch/batchAccounts",
                                                     SHARED_RESOURCE,
                                                     false
      )
      relayNamespace <- getLandingZoneResourceName(groupedLzResources,
                                                   "Microsoft.Relay/namespaces",
                                                   SHARED_RESOURCE,
                                                   false
      )
      storageAccountName <- getLandingZoneResourceName(groupedLzResources,
                                                       "Microsoft.Storage/storageAccounts",
                                                       SHARED_RESOURCE,
                                                       false
      )
      applicationInsightsName <- getLandingZoneResourceName(groupedLzResources,
                                                            "Microsoft.Insights/components",
                                                            SHARED_RESOURCE,
                                                            false
      )
      vnetName <- getLandingZoneResourceName(groupedLzResources, "DeployedSubnet", AKS_NODE_POOL_SUBNET, true)
      batchNodesSubnetName <- getLandingZoneResourceName(groupedLzResources,
                                                         "DeployedSubnet",
                                                         WORKSPACE_BATCH_SUBNET,
                                                         false
      )
      aksSubnetName <- getLandingZoneResourceName(groupedLzResources, "DeployedSubnet", AKS_NODE_POOL_SUBNET, false)
      postgresName <- getLandingZoneResourceName(groupedLzResources,
                                                 "Microsoft.DBforPostgreSQL/flexibleServers",
                                                 SHARED_RESOURCE,
                                                 false
      ).attempt
    } yield LandingZoneResources(
      landingZoneId,
      AKSClusterName(aksClusterName),
      BatchAccountName(batchAccountName),
      RelayNamespace(relayNamespace),
      StorageAccountName(storageAccountName),
      NetworkName(vnetName),
      SubnetworkName(batchNodesSubnetName),
      SubnetworkName(aksSubnetName),
      region,
      ApplicationInsightsName(applicationInsightsName),
      postgresName.toOption.map(PostgresName)
    )

  private def getLandingZoneResourceName(
    landingZoneResourcesByPurpose: Map[(LandingZoneResourcePurpose, String), List[LandingZoneResource]],
    resourceType: String,
    purpose: LandingZoneResourcePurpose,
    useParent: Boolean
  ): F[String] =
    landingZoneResourcesByPurpose
      .get((purpose, resourceType.toLowerCase))
      .flatMap(_.headOption)
      .flatMap { r =>
        if (useParent) r.resourceParentId.flatMap(_.split('/').lastOption)
        else r.resourceName.orElse(r.resourceId.flatMap(_.split('/').lastOption))
      }
      .fold(
        F.raiseError[String](
          AppCreationException(s"${resourceType} resource with purpose ${purpose} not found in landing zone")
        )
      )(F.pure)

  private def getLandingZone(billingProfileId: String, authorization: Authorization)(implicit
    ev: Ask[F, AppContext]
  ): F[Option[LandingZone]] =
    for {
      ctx <- ev.ask
      res <- httpClient.expectOptionOr[ListLandingZonesResult](
        Request[F](
          method = Method.GET,
          uri = config.uri
            .withPath(Uri.Path.unsafeFromString("/api/landingzones/v1/azure"))
            .withQueryParam("billingProfileId", billingProfileId),
          headers = headers(authorization, ctx.traceId, withBody = false)
        )
      )(onError)
      landingZoneOption = res.flatMap(listLandingZoneResult => listLandingZoneResult.landingzones.headOption)
    } yield landingZoneOption

  private def listLandingZoneResourcesByType(landingZoneId: UUID, authorization: Authorization)(implicit
    ev: Ask[F, AppContext]
  ): F[List[LandingZoneResourcesByPurpose]] =
    for {
      ctx <- ev.ask
      resOpt <- httpClient.expectOptionOr[ListLandingZoneResourcesResult](
        Request[F](
          method = Method.GET,
          uri = config.uri
            .withPath(
              Uri.Path
                .unsafeFromString(s"/api/landingzones/v1/azure/${landingZoneId}/resources")
            ),
          headers = headers(authorization, ctx.traceId, withBody = false)
        )
      )(onError)
    } yield resOpt.fold(List.empty[LandingZoneResourcesByPurpose])(res => res.resources)

  override def deleteVm(request: DeleteWsmResourceRequest, authorization: Authorization)(implicit
    ev: Ask[F, AppContext]
  ): F[Option[DeleteWsmResourceResult]] =
    deleteHelper(request, authorization, "vm")

  override def deleteStorageContainer(request: DeleteWsmResourceRequest, authorization: Authorization)(implicit
    ev: Ask[F, AppContext]
  ): F[Option[DeleteWsmResourceResult]] =
    deleteHelper(request, authorization, "storageContainer")

  override def deleteDisk(request: DeleteWsmResourceRequest, authorization: Authorization)(implicit
    ev: Ask[F, AppContext]
  ): F[Option[DeleteWsmResourceResult]] =
    deleteHelper(request, authorization, "disks")

  override def getCreateVmJobResult(request: GetJobResultRequest, authorization: Authorization)(implicit
    ev: Ask[F, AppContext]
  ): F[GetCreateVmJobResult] =
    for {
      ctx <- ev.ask
      res <- httpClient.expectOr[GetCreateVmJobResult](
        Request[F](
          method = Method.GET,
          uri = config.uri
            .withPath(
              Uri.Path
                .unsafeFromString(
                  s"/api/workspaces/v1/${request.workspaceId.value.toString}/resources/controlled/azure/vm/create-result/${request.jobId.value}"
                )
            ),
          headers = headers(authorization, ctx.traceId, false)
        )
      )(onError)
    } yield res

  override def getDeleteVmJobResult(request: GetJobResultRequest, authorization: Authorization)(implicit
    ev: Ask[F, AppContext]
  ): F[GetDeleteJobResult] =
    for {
      ctx <- ev.ask
      res <- httpClient.expectOr[GetDeleteJobResult](
        Request[F](
          method = Method.GET,
          uri = config.uri
            .withPath(
              Uri.Path
                .unsafeFromString(
                  s"/api/workspaces/v1/${request.workspaceId.value.toString}/resources/controlled/azure/vm/delete-result/${request.jobId.value}"
                )
            ),
          headers = headers(authorization, ctx.traceId, false)
        )
      )(onError)
    } yield res

  override def getDeleteDiskJobResult(request: GetJobResultRequest, authorization: Authorization)(implicit
    ev: Ask[F, AppContext]
  ): F[GetDeleteJobResult] =
    for {
      ctx <- ev.ask
      res <- httpClient.expectOr[GetDeleteJobResult](
        Request[F](
          method = Method.GET,
          uri = config.uri
            .withPath(
              Uri.Path
                .unsafeFromString(
                  s"/api/workspaces/v1/${request.workspaceId.value.toString}/resources/controlled/azure/disks/delete-result/${request.jobId.value}"
                )
            ),
          headers = headers(authorization, ctx.traceId, false)
        )
      )(onError)
    } yield res

  override def getWorkspaceStorageContainer(workspaceId: WorkspaceId, authorization: Authorization)(implicit
    ev: Ask[F, AppContext]
  ): F[Option[StorageContainerResponse]] = for {
    resp <- getWorkspaceResourceHelper(workspaceId, authorization, WsmResourceType.AzureStorageContainer)
    res = resp.resources.collect {
      case r
          if r.resourceAttributes
            .isInstanceOf[ResourceAttributes.StorageContainerResourceAttributes] && r.resourceAttributes
            .asInstanceOf[ResourceAttributes.StorageContainerResourceAttributes]
            .name
            .value
            .startsWith("sc-") => // by WSM workspace storage container naming convention
        val rr = r.resourceAttributes
          .asInstanceOf[ResourceAttributes.StorageContainerResourceAttributes]
        StorageContainerResponse(rr.name, r.metadata.resourceId)
    }.headOption
  } yield res

  private def getWorkspaceResourceHelper(workspaceId: WorkspaceId,
                                         authorization: Authorization,
                                         wsmResourceType: WsmResourceType
  )(implicit
    ev: Ask[F, AppContext]
  ): F[GetWsmResourceResponse] = for {
    ctx <- ev.ask
    resp <- httpClient.expectOr[GetWsmResourceResponse](
      Request[F](
        method = Method.GET,
        uri = config.uri
          .withPath(
            Uri.Path
              .unsafeFromString(
                s"/api/workspaces/v1/${workspaceId.value}/resources"
              )
          )
          .withMultiValueQueryParams(
            Map(
              "resource" -> List(wsmResourceType.toString),
              "stewardship" -> List("CONTROLLED"),
              "limit" -> List("500")
            )
          ),
        headers = headers(authorization, ctx.traceId, false)
      )
    )(onError)
  } yield resp

  private def deleteHelper(req: DeleteWsmResourceRequest, authorization: Authorization, resource: String)(implicit
    ev: Ask[F, AppContext]
  ): F[Option[DeleteWsmResourceResult]] =
    for {
      ctx <- ev.ask
      res <- httpClient.expectOptionOr[DeleteWsmResourceResult](
        Request[F](
          method = Method.POST,
          uri = config.uri
            .withPath(
              Uri.Path
                .unsafeFromString(
                  s"/api/workspaces/v1/${req.workspaceId.value.toString}/resources/controlled/azure/${resource}/${req.resourceId.value.toString}"
                )
            ),
          entity = req.deleteRequest,
          headers = headers(authorization, ctx.traceId, true)
        )
      )(onError)
    } yield res

  private def onError(response: Response[F])(implicit ev: Ask[F, AppContext]): F[Throwable] =
    for {
      context <- ev.ask
      body <- response.bodyText.compile.foldMonoid
      _ <- logger.error(context.loggingCtx)(s"WSM call failed: $body")
      _ <- metrics.incrementCounter("wsm/errorResponse")
    } yield WsmException(context.traceId, body)

  def headers(authorization: Authorization, traceId: TraceId, withBody: Boolean): Headers = {
    val requestId = Header.Raw(CIString("X-Request-ID"), traceId.asString)
    if (withBody)
      Headers(authorization, defaultMediaType, requestId)
    else
      Headers(authorization, requestId)
  }
}
