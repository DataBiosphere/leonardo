package org.broadinstitute.dsde.workbench.leonardo.dao

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import io.circe.syntax.EncoderOps
import io.circe.{Encoder, Printer}
import org.broadinstitute.dsde.workbench.azure.{ApplicationInsightsName, BatchAccountName, RelayNamespace}
import org.broadinstitute.dsde.workbench.google2.{NetworkName, SubnetworkName}
import org.broadinstitute.dsde.workbench.leonardo.TestUtils.appContext
import org.broadinstitute.dsde.workbench.leonardo.config.HttpWsmDaoConfig
import org.broadinstitute.dsde.workbench.leonardo.dao.LandingZoneResourcePurpose.{
  AKS_NODE_POOL_SUBNET,
  LandingZoneResourcePurpose,
  POSTGRESQL_SUBNET,
  SHARED_RESOURCE,
  WORKSPACE_BATCH_SUBNET,
  WORKSPACE_COMPUTE_SUBNET
}
import org.broadinstitute.dsde.workbench.leonardo.{
  AKSCluster,
  BillingProfileId,
  LandingZoneResources,
  LeonardoTestSuite,
  PostgresServer,
  StorageAccountName,
  WorkspaceId,
  WsmControlledResourceId,
  WsmJobId
}
import org.http4s._
import org.http4s.client.Client
import org.http4s.headers.Authorization
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec

import java.util.UUID

class HttpWsmDaoSpec extends AnyFlatSpec with LeonardoTestSuite with BeforeAndAfterAll {
  val config = HttpWsmDaoConfig(Uri.unsafeFromString("127.0.0.1"))

  it should "not error when getting 404 during resource deletion" in {
    val wsmClient = Client.fromHttpApp[IO](
      HttpApp(_ => IO(Response(status = Status.NotFound)))
    )

    val wsmDao = new HttpWsmDao[IO](wsmClient, config)
    val res = wsmDao
      .deleteVm(
        DeleteWsmResourceRequest(WorkspaceId(UUID.randomUUID()),
                                 WsmControlledResourceId(UUID.randomUUID()),
                                 WsmDaoDeleteControlledAzureResourceRequest(WsmJobControl(WsmJobId("job")))
        ),
        Authorization(Credentials.Token(AuthScheme.Bearer, "dummy"))
      )
      .attempt

    res.unsafeRunSync().isRight shouldBe true
  }

  val testCases: Map[Option[Map[String, String]], Boolean] = Map(
    None -> false,
    Option(Map.empty[String, String]) -> false,
    Option(Map("not-pgbouncer" -> "true")) -> false,
    Option(Map("pgbouncer-enabled" -> "false", "another-tag" -> "true")) -> false,
    Option(Map("pgbouncer-enabled" -> "not-a-boolean", "another-tag" -> "true")) -> false,
    Option(Map("pgbouncer-enabled" -> "true", "not-pgbouncer" -> "false")) -> true
  )

  testCases.foreach { case (tags, expectedPgBouncer) =>
    it should s"correctly get landing zone resources and detect PgBouncer for tags: $tags" in {
      val billingId = UUID.randomUUID()
      val landingZoneId = UUID.randomUUID()

      val originalLandingZone = LandingZone(landingZoneId, billingId, "def", "v1", "2022-11-11")
      val landingZoneResponse = ListLandingZonesResult(List(originalLandingZone))
      val landingZoneStringResponse = landingZoneResponse.asJson.printWith(Printer.noSpaces)

      val originalLandingZoneResourcesByPurpose = List(
        LandingZoneResourcesByPurpose(
          SHARED_RESOURCE,
          List(
            buildMockLandingZoneResource("Microsoft.ContainerService/managedClusters", "lzcluster"),
            buildMockLandingZoneResource("Microsoft.Batch/batchAccounts", "lzbatch"),
            buildMockLandingZoneResource("Microsoft.Relay/namespaces", "lznamespace"),
            buildMockLandingZoneResource("Microsoft.Storage/storageAccounts", "lzstorage"),
            buildMockLandingZoneResource("microsoft.dbforpostgresql/flexibleservers", "lzpostgres", tags = tags),
            buildMockLandingZoneResource("microsoft.operationalinsights/workspaces", "lzloganalytics"),
            buildMockLandingZoneResource("Microsoft.Insights/components", "lzappinsights")
          )
        ),
        LandingZoneResourcesByPurpose(
          WORKSPACE_BATCH_SUBNET,
          List(
            buildMockLandingZoneResource("DeployedSubnet", "batchsub")
          )
        ),
        LandingZoneResourcesByPurpose(
          AKS_NODE_POOL_SUBNET,
          List(
            buildMockLandingZoneResource("DeployedSubnet", "akssub")
          )
        ),
        LandingZoneResourcesByPurpose(
          WORKSPACE_COMPUTE_SUBNET,
          List(
            buildMockLandingZoneResource("DeployedSubnet", "computesub")
          )
        ),
        LandingZoneResourcesByPurpose(
          POSTGRESQL_SUBNET,
          List(buildMockLandingZoneResource("DeployedSubnet", "postgressub"))
        )
      )
      val landingZoneResourcesResult =
        ListLandingZoneResourcesResult(landingZoneId, originalLandingZoneResourcesByPurpose)
      val landingZoneResourcesStringResponse = landingZoneResourcesResult.asJson.printWith(Printer.noSpaces)

      val wsmClient = Client.fromHttpApp[IO](
        HttpApp { request =>
          val landingZoneRequestString = s"/api/landingzones/v1/azure?billingProfileId=${billingId}"
          val resourceRequestString = s"/api/landingzones/v1/azure/${landingZoneId}/resources"
          request.uri.renderString match {
            case `landingZoneRequestString` =>
              IO(Response(status = Status.Ok).withEntity(landingZoneStringResponse))
            case `resourceRequestString` =>
              IO(Response(status = Status.Ok).withEntity(landingZoneResourcesStringResponse))
          }
        }
      )

      val wsmDao = new HttpWsmDao[IO](wsmClient, config)
      val res = wsmDao
        .getLandingZoneResources(
          BillingProfileId(billingId.toString),
          Authorization(Credentials.Token(AuthScheme.Bearer, "dummy"))
        )
        .attempt
        .unsafeRunSync()

      res.isRight shouldBe true

      val expectedLandingZoneResources = LandingZoneResources(
        landingZoneId,
        AKSCluster("lzcluster", Map("aks-cost-vpa-enabled" -> false)),
        BatchAccountName("lzbatch"),
        RelayNamespace("lznamespace"),
        StorageAccountName("lzstorage"),
        NetworkName("lzvnet"),
        SubnetworkName("id-prefix/batchsub"),
        SubnetworkName("akssub"),
        com.azure.core.management.Region.US_EAST,
        ApplicationInsightsName("lzappinsights"),
        Some(PostgresServer("lzpostgres", expectedPgBouncer))
      )

      val landingZoneResources = res.toOption.get
      landingZoneResources shouldBe expectedLandingZoneResources
    }
  }

  private def buildMockLandingZoneResource(resourceType: String,
                                           resourceName: String,
                                           useId: Boolean = true,
                                           tags: Option[Map[String, String]] = None
  ) =
    LandingZoneResource(
      resourceId = if (useId) Some(s"id-prefix/${resourceName}") else None,
      resourceType,
      resourceName = if (resourceType.equalsIgnoreCase("DeployedSubnet")) Some(resourceName) else None,
      resourceParentId = if (resourceType.equalsIgnoreCase("DeployedSubnet")) Some("lzvnet") else None,
      region = com.azure.core.management.Region.US_EAST.toString,
      tags
    )

  implicit val landingZoneEncoder: Encoder[LandingZone] =
    Encoder.forProduct5("landingZoneId", "billingProfileId", "definition", "version", "createdDate")(x =>
      (x.landingZoneId, x.billingProfileId, x.definition, x.version, x.createdDate)
    )
  implicit val listLandingZonesResultEncoder: Encoder[ListLandingZonesResult] =
    Encoder.forProduct1("landingzones")(x => x.landingzones)

  implicit val landingZoneResourceEncoder: Encoder[LandingZoneResource] =
    Encoder.forProduct6("resourceId", "resourceType", "resourceName", "resourceParentId", "region", "tags")(x =>
      (x.resourceId, x.resourceType, x.resourceName, x.resourceParentId, x.region, x.tags)
    )
  implicit val landingZoneResourcePurposeEncoder: Encoder[LandingZoneResourcePurpose] =
    Encoder.encodeString.contramap(_.toString)
  implicit val landingZoneResourcesByPurposeEncoder: Encoder[LandingZoneResourcesByPurpose] =
    Encoder.forProduct2("purpose", "deployedResources")(x => (x.purpose, x.deployedResources))
  implicit val listLandingZoneResourcesResultEncoder: Encoder[ListLandingZoneResourcesResult] =
    Encoder.forProduct2("id", "resources")(x => (x.id, x.resources))
}
