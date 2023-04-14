package org.broadinstitute.dsde.workbench.leonardo.dao

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import io.circe.parser._
import org.broadinstitute.dsde.workbench.leonardo.TestUtils.appContext
import org.broadinstitute.dsde.workbench.leonardo.dao.HttpWdsDAO.statusDecoder
import org.broadinstitute.dsde.workbench.leonardo.{AppType, LeonardoTestSuite}
import org.http4s._
import org.http4s.circe.CirceEntityEncoder._
import org.http4s.client.Client
import org.http4s.headers.Authorization
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class HttpWdsDAOSpec extends AnyFlatSpec with Matchers with LeonardoTestSuite {
  "HttpWdsDAO" should "decode wds status endpoint response successfully" in {
    val response =
      """
        |{
        |  "status": "UP"
        |}
      """.stripMargin

    val res = for {
      json <- parse(response)
      resp <- json.as[WdsStatusCheckResponse]
    } yield resp

    res shouldBe (Right(WdsStatusCheckResponse("UP")))
  }

  "HttpWdsDAO.getStatus" should "return false if status is not ok" in {
    val authHeader = Authorization(Credentials.Token(AuthScheme.Bearer, "token"))

    val response =
      """
        |{
        |  "status": "DOWN"
        |}
      """.stripMargin

    val okWds = Client.fromHttpApp[IO](
      HttpApp(_ => IO.fromEither(parse(response)).flatMap(r => IO(Response(status = Status.Ok).withEntity(r))))
    )

    val wdsDAO = new HttpWdsDAO(okWds)
    val res = wdsDAO.getStatus(Uri.unsafeFromString("https://test.com/wds/status"), authHeader, AppType.Cromwell)
    val status = res.unsafeRunSync()
    status shouldBe false
  }

  "HttpWdsDAO.getStatus" should "return true if status is ok for both cromwell and WDS appTypes" in {
    val authHeader = Authorization(Credentials.Token(AuthScheme.Bearer, "token"))

    val upResponse =
      """
        |{
        |  "status": "UP"
        |}
      """.stripMargin

    val downResponse =
      """
        |{
        |  "status": "DOWN"
        |}
      """.stripMargin

    val baseUri = "https://test.com"
    val coaWds = Client.fromHttpApp[IO](
      HttpApp { request =>
        val statusEndpoint = s"$baseUri/wds/status"
        request.uri.renderString match {
          case `statusEndpoint` =>
            IO.fromEither(parse(upResponse)).flatMap(r => IO(Response(status = Status.Ok).withEntity(r)))
          case _ => IO.fromEither(parse(downResponse)).flatMap(r => IO(Response(status = Status.Ok).withEntity(r)))
        }
      }
    )

    val singleWds = Client.fromHttpApp[IO](
      HttpApp { request =>
        val statusEndpoint = s"$baseUri/status"
        request.uri.renderString match {
          case `statusEndpoint` =>
            IO.fromEither(parse(upResponse)).flatMap(r => IO(Response(status = Status.Ok).withEntity(r)))
          case _ => IO.fromEither(parse(downResponse)).flatMap(r => IO(Response(status = Status.Ok).withEntity(r)))
        }
      }
    )

    // First test the COA WDS
    val coaWdsDAO = new HttpWdsDAO(coaWds)

    // Status succeeds when app type is cromwell
    val coaRes = coaWdsDAO.getStatus(Uri.unsafeFromString(baseUri), authHeader, AppType.Cromwell)
    val cosStatus = coaRes.unsafeRunSync()
    cosStatus shouldBe true

    // Status fails when we wrongly put apptype as WDS
    val coaResBad = coaWdsDAO.getStatus(Uri.unsafeFromString(baseUri), authHeader, AppType.Wds)
    val cosStatusBad = coaResBad.unsafeRunSync()
    cosStatusBad shouldBe false

    // Next test multi-app WDS
    val singleWdsDAO = new HttpWdsDAO(singleWds)

    // Status succeeds when app type is WDS
    val wdsRes = singleWdsDAO.getStatus(Uri.unsafeFromString(baseUri), authHeader, AppType.Wds)
    val wdsStatus = wdsRes.unsafeRunSync()
    wdsStatus shouldBe true

    // Status fails when we wrongly put appType as cromwell
    val wdsResBad = singleWdsDAO.getStatus(Uri.unsafeFromString(baseUri), authHeader, AppType.Cromwell)
    val wdsStatusBad = wdsResBad.unsafeRunSync()
    wdsStatusBad shouldBe false
  }
}
