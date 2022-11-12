package org.broadinstitute.dsde.workbench.leonardo.dao

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import io.circe.Printer
import io.circe.syntax.EncoderOps
import org.broadinstitute.dsde.workbench.leonardo.TestUtils.appContext
import org.broadinstitute.dsde.workbench.leonardo.config.HttpWsmDaoConfig
import org.broadinstitute.dsde.workbench.leonardo.dao.WsmDecoders.{
  listLandingZoneResourcesResultEncoder,
  listLandingZonesResultEncoder
}
import org.broadinstitute.dsde.workbench.leonardo.{LeonardoTestSuite, WorkspaceId, WsmControlledResourceId, WsmJobId}
import org.http4s.client.Client
import org.http4s.headers.Authorization
import org.http4s._
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

  it should "correctly parse landing zone id data" in {
    val originalLandingZone = LandingZone(
      UUID.fromString("910f1c68-425d-4060-94f2-cb57f08425fe"),
      UUID.fromString("910f1c68-425d-4060-94f2-cb57f08425fe"),
      "def",
      "v1",
      "2022-11-11"
    )
    val response = ListLandingZonesResult(List(originalLandingZone))
    val stringResponse = response.asJson.printWith(Printer.noSpaces)

    val wsmClient = Client.fromHttpApp[IO](
      HttpApp(_ => IO(Response(status = Status.Ok).withEntity(stringResponse)))
    )

    val wsmDao = new HttpWsmDao[IO](wsmClient, config)
    val res = wsmDao
      .getLandingZone(
        "78bacb57-2d47-4ac2-8710-5bd12edbc1bf",
        Authorization(Credentials.Token(AuthScheme.Bearer, "dummy"))
      )
      .attempt
      .unsafeRunSync()

    res.isRight shouldBe true

    val landingZone = res.toOption.flatten.get
    landingZone shouldBe originalLandingZone
  }

  it should "correctly list landing zone resources" in {
    val landingZoneId = UUID.fromString("78bacb57-2d47-4ac2-8710-5bd12edbc1bf")
    val originalLandingZoneResourcesByPurpose = List(
      LandingZoneResourcesByPurpose(
        "SHARED_RESOURCE",
        List(
          LandingZoneResource(
            "id",
            "type",
            "name",
            "parent-id",
            "us-east"
          )
        )
      )
    )
    val response = ListLandingZoneResourcesResult(landingZoneId, originalLandingZoneResourcesByPurpose)
    val stringResponse = response.asJson.printWith(Printer.noSpaces)

    val wsmClient = Client.fromHttpApp[IO](
      HttpApp(_ => IO(Response(status = Status.Ok).withEntity(stringResponse)))
    )

    val wsmDao = new HttpWsmDao[IO](wsmClient, config)
    val res = wsmDao
      .listLandingZoneResourcesByType(
        landingZoneId,
        Authorization(Credentials.Token(AuthScheme.Bearer, "dummy"))
      )
      .attempt
      .unsafeRunSync()

    res.isRight shouldBe true

    val landingZoneResourcesByPurpose = res.toOption.flatten.get
    landingZoneResourcesByPurpose shouldBe originalLandingZoneResourcesByPurpose
  }
}
