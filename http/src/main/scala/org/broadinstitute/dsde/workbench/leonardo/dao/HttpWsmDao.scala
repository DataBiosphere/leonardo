package org.broadinstitute.dsde.workbench.leonardo
package dao

import cats.data.OptionT
import cats.effect.Async
import cats.implicits._
import cats.mtl.Ask
import org.broadinstitute.dsde.workbench.azure.{ApplicationInsightsName, BatchAccountName, RelayNamespace}
import org.broadinstitute.dsde.workbench.google2.{NetworkName, SubnetworkName}
import org.broadinstitute.dsde.workbench.leonardo.config.HttpWsmDaoConfig
import org.broadinstitute.dsde.workbench.leonardo.dao.LandingZoneResourcePurpose.{
  AKS_NODE_POOL_SUBNET,
  LandingZoneResourcePurpose,
  SHARED_RESOURCE,
  WORKSPACE_BATCH_SUBNET
}
import org.broadinstitute.dsde.workbench.leonardo.dao.WsmDecoders._
import org.broadinstitute.dsde.workbench.leonardo.db.WsmResourceType
import org.broadinstitute.dsde.workbench.leonardo.util.AppCreationException
import org.broadinstitute.dsde.workbench.model.TraceId
import org.broadinstitute.dsde.workbench.openTelemetry.OpenTelemetryMetrics
import org.http4s._
import org.http4s.circe.CirceEntityDecoder._
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

  override def getLandingZoneResources(billingProfileId: BillingProfileId, userToken: Authorization)(implicit
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
      aksResource <- getLandingZoneResource(groupedLzResources,
                                            "Microsoft.ContainerService/managedClusters",
                                            SHARED_RESOURCE
      )
      aksCluster <- getLandingZoneResourceName(aksResource, useParent = false).map { aksName =>
        val tagValue = getLandingZoneResourceTagValue(aksResource, "aks-cost-vpa-enabled")
        val tags: Map[String, Boolean] =
          Map("aks-cost-vpa-enabled" -> java.lang.Boolean.parseBoolean(tagValue.getOrElse("false")))
        AKSCluster(aksName, tags)
      }
      _ <- logger.info(s"Retrieved Landing Zone AKS cluster: ${aksCluster}")
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
      batchNodesSubnetName <- getLandingZoneResourceId(groupedLzResources, "DeployedSubnet", WORKSPACE_BATCH_SUBNET)
      aksSubnetName <- getLandingZoneResourceName(groupedLzResources, "DeployedSubnet", AKS_NODE_POOL_SUBNET, false)
      postgresResource <- getLandingZoneResource(groupedLzResources,
                                                 "Microsoft.DBforPostgreSQL/flexibleServers",
                                                 SHARED_RESOURCE
      ).attempt // use attempt here because older Landing Zones do not have a Postgres server
      postgresServer <- postgresResource.toOption.traverse { resource =>
        getLandingZoneResourceName(resource, useParent = false).map { pgName =>
          val tagValue = getLandingZoneResourceTagValue(resource, "pgbouncer-enabled")
          val pgBouncerEnabled: Boolean = java.lang.Boolean.parseBoolean(tagValue.getOrElse("false"))
          logger.info(
            s"Landing Zone Postgres server has 'pgbouncer-enabled' tag $tagValue; setting pgBouncerEnabled to $pgBouncerEnabled."
          )
          PostgresServer(pgName, pgBouncerEnabled)
        }
      }
    } yield LandingZoneResources(
      landingZoneId,
      aksCluster,
      BatchAccountName(batchAccountName),
      RelayNamespace(relayNamespace),
      StorageAccountName(storageAccountName),
      NetworkName(vnetName),
      SubnetworkName(batchNodesSubnetName),
      SubnetworkName(aksSubnetName),
      region,
      ApplicationInsightsName(applicationInsightsName),
      postgresServer
    )

  /**
   * Given a LandingZoneResource, retrieve the value of a specific tag on that resource.
   *
   * @param resource the LZ resource to inspect
   * @param tagName  name of the tag whose value to return
   * @return the tag's value, or None if the tag is not present on the resource
   */
  private def getLandingZoneResourceTagValue(resource: LandingZoneResource, tagName: String): Option[String] =
    resource.tags.flatMap(_.get(tagName))

  /**
   * Given a collection of landing zone resources by purpose, return the single resource that
   * matches a given resource type and purpose. Throws an error if the resource is not found.
   *
   * @param landingZoneResourcesByPurpose the collection in which to search
   * @param resourceType                  type of resource to return
   * @param purpose                       purpose of resource to return
   * @return the LandingZoneResource
   */
  private def getLandingZoneResource(
    landingZoneResourcesByPurpose: Map[(LandingZoneResourcePurpose, String), List[LandingZoneResource]],
    resourceType: String,
    purpose: LandingZoneResourcePurpose
  ): F[LandingZoneResource] =
    landingZoneResourcesByPurpose
      .get((purpose, resourceType.toLowerCase))
      .flatMap(_.headOption)
      .fold(
        F.raiseError[LandingZoneResource](
          AppCreationException(s"${resourceType} resource with purpose ${purpose} not found in landing zone")
        )
      )(F.pure)

  /**
   * Given a collection of landing zone resources by purpose, return the name of the single resource that
   * matches a given resource type and purpose. Throws an error if the resource is not found.
   *
   * @param landingZoneResourcesByPurpose the collection in which to search
   * @param resourceType                  type of resource to return
   * @param purpose                       purpose of resource to return
   * @param useParent                     whether to return the resource's name or its parent's name
   * @return name of the specified resource
   */
  private def getLandingZoneResourceName(
    landingZoneResourcesByPurpose: Map[(LandingZoneResourcePurpose, String), List[LandingZoneResource]],
    resourceType: String,
    purpose: LandingZoneResourcePurpose,
    useParent: Boolean
  ): F[String] =
    for {
      resource <- getLandingZoneResource(landingZoneResourcesByPurpose, resourceType, purpose)
      name <- getLandingZoneResourceName(resource, useParent)
    } yield name

  /**
   * Given a landing zone resource, return that resource's name. Throws an error if the name is not found.
   *
   * @param resource  the resource whose name to return
   * @param useParent whether to return the resource's name or its parent's name
   * @return name of the resource
   */
  private def getLandingZoneResourceName(resource: LandingZoneResource, useParent: Boolean): F[String] =
    OptionT
      .fromOption[F](
        if (useParent) resource.resourceParentId.flatMap(_.split('/').lastOption)
        else resource.resourceName.orElse(resource.resourceId.flatMap(_.split('/').lastOption))
      )
      .getOrRaise(
        AppCreationException(s"could not determine name for resource $resource")
      )

  private def getLandingZoneResourceId(
    landingZoneResourcesByPurpose: Map[(LandingZoneResourcePurpose, String), List[LandingZoneResource]],
    resourceType: String,
    purpose: LandingZoneResourcePurpose
  ): F[String] =
    for {
      resource <- getLandingZoneResource(landingZoneResourcesByPurpose, resourceType, purpose)
      id <- OptionT
        .fromOption[F](
          resource.resourceId
        )
        .getOrRaise(
          AppCreationException(s"could not determine id for resource $resource")
        )
    } yield id

  private def getLandingZone(billingProfileId: BillingProfileId, authorization: Authorization)(implicit
    ev: Ask[F, AppContext]
  ): F[Option[LandingZone]] =
    for {
      ctx <- ev.ask
      res <- httpClient.expectOptionOr[ListLandingZonesResult](
        Request[F](
          method = Method.GET,
          uri = config.uri
            .withPath(Uri.Path.unsafeFromString("/api/landingzones/v1/azure"))
            .withQueryParam("billingProfileId", billingProfileId.value),
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
