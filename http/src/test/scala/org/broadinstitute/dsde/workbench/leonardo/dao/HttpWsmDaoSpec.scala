package org.broadinstitute.dsde.workbench.leonardo.dao

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import io.circe.syntax.EncoderOps
import io.circe.{Encoder, Printer}
import org.broadinstitute.dsde.workbench.azure.{
  AKSClusterName,
  ApplicationInsightsName,
  BatchAccountName,
  RelayNamespace
}
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
  LandingZoneResources,
  LeonardoTestSuite,
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
                                 DeleteControlledAzureResourceRequest(WsmJobControl(WsmJobId("job")))
        ),
        Authorization(Credentials.Token(AuthScheme.Bearer, "dummy"))
      )
      .attempt

    res.unsafeRunSync().isRight shouldBe true
  }

  it should "correctly get landing zone resources" in {
    val billingId = UUID.fromString("78bacb57-2d47-4ac2-8710-5bd12edbc1bf")
    val landingZoneId = UUID.fromString("910f1c68-425d-4060-94f2-cb57f08425fe")

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
          buildMockLandingZoneResource("microsoft.dbforpostgresql/flexibleservers", "lzpostgres"),
          buildMockLandingZoneResource("microsoft.operationalinsights/workspaces", "lzloganalytics"),
          buildMockLandingZoneResource("Microsoft.Insights/components", "lzappinsights")
        )
      ),
      LandingZoneResourcesByPurpose(
        WORKSPACE_BATCH_SUBNET,
        List(
          buildMockLandingZoneResource("DeployedSubnet", "batchsub", false)
        )
      ),
      LandingZoneResourcesByPurpose(
        AKS_NODE_POOL_SUBNET,
        List(
          buildMockLandingZoneResource("DeployedSubnet", "akssub", false)
        )
      ),
      LandingZoneResourcesByPurpose(
        WORKSPACE_COMPUTE_SUBNET,
        List(
          buildMockLandingZoneResource("DeployedSubnet", "computesub", false)
        )
      ),
      LandingZoneResourcesByPurpose(
        POSTGRESQL_SUBNET,
        List(buildMockLandingZoneResource("DeployedSubnet", "postgressub", false))
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
        billingId.toString,
        Authorization(Credentials.Token(AuthScheme.Bearer, "dummy"))
      )
      .attempt
      .unsafeRunSync()

    res.isRight shouldBe true

    val expectedLandingZoneResources = LandingZoneResources(
      UUID.fromString("910f1c68-425d-4060-94f2-cb57f08425fe"),
      AKSClusterName("lzcluster"),
      BatchAccountName("lzbatch"),
      RelayNamespace("lznamespace"),
      StorageAccountName("lzstorage"),
      NetworkName("lzvnet"),
      SubnetworkName("batchsub"),
      SubnetworkName("akssub"),
      com.azure.core.management.Region.US_EAST,
      ApplicationInsightsName("lzappinsights")
    )

    val landingZoneResources = res.toOption.get
    landingZoneResources shouldBe expectedLandingZoneResources
  }

  private def buildMockLandingZoneResource(resourceType: String, resourceName: String, useId: Boolean = true) =
    LandingZoneResource(
      resourceId = if (useId) Some(s"id-prefix/${resourceName}") else None,
      resourceType,
      resourceName = if (useId) None else Some(resourceName),
      resourceParentId = if (useId) None else Some("lzvnet"),
      region = com.azure.core.management.Region.US_EAST.toString
    )

  implicit val landingZoneEncoder: Encoder[LandingZone] =
    Encoder.forProduct5("landingZoneId", "billingProfileId", "definition", "version", "createdDate")(x =>
      (x.landingZoneId, x.billingProfileId, x.definition, x.version, x.createdDate)
    )
  implicit val listLandingZonesResultEncoder: Encoder[ListLandingZonesResult] =
    Encoder.forProduct1("landingzones")(x => x.landingzones)

  implicit val landingZoneResourceEncoder: Encoder[LandingZoneResource] =
    Encoder.forProduct5("resourceId", "resourceType", "resourceName", "resourceParentId", "region")(x =>
      (x.resourceId, x.resourceType, x.resourceName, x.resourceParentId, x.region)
    )
  implicit val landingZoneResourcePurposeEncoder: Encoder[LandingZoneResourcePurpose] =
    Encoder.encodeString.contramap(_.toString)
  implicit val landingZoneResourcesByPurposeEncoder: Encoder[LandingZoneResourcesByPurpose] =
    Encoder.forProduct2("purpose", "deployedResources")(x => (x.purpose, x.deployedResources))
  implicit val listLandingZoneResourcesResultEncoder: Encoder[ListLandingZoneResourcesResult] =
    Encoder.forProduct2("id", "resources")(x => (x.id, x.resources))
}
