package org.broadinstitute.dsde.workbench.leonardo.dao

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import io.circe.parser._
import org.broadinstitute.dsde.workbench.leonardo.{AppType, LeonardoTestSuite}
import org.broadinstitute.dsde.workbench.leonardo.TestUtils.appContext
import org.broadinstitute.dsde.workbench.leonardo.dao.HttpWdsDAO.statusDecoder
import org.broadinstitute.dsde.workbench.leonardo.db.TestComponent
import org.http4s._
import org.http4s.circe.CirceEntityEncoder._
import org.http4s.client.Client
import org.http4s.headers.Authorization
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class HttpWdsDAOSpec extends AnyFlatSpec with Matchers with LeonardoTestSuite with TestComponent {
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
        |  "status": "DOWN"
        |}
      """.stripMargin

    val downResponse =
      """
        |{
        |  "status": "UP"
        |}
      """.stripMargin

    val coaWds = Client.fromHttpApp[IO](
      HttpApp { request =>
        request.uri.renderString match {
          case "/wds/status" => IO.fromEither(parse(upResponse)).flatMap(r => IO(Response(status = Status.Ok).withEntity(r)))
          case _ => IO.fromEither(parse(downResponse)).flatMap(r => IO(Response(status = Status.Ok).withEntity(r)))
        }
      }
    )

    val singleWds = Client.fromHttpApp[IO](
      HttpApp { request =>
        request.uri.renderString match {
          case "/status" => IO.fromEither(parse(upResponse)).flatMap(r => IO(Response(status = Status.Ok).withEntity(r)))
          case _ => IO.fromEither(parse(downResponse)).flatMap(r => IO(Response(status = Status.Ok).withEntity(r)))
        }
      }
    )

    val coaWdsDAO = new HttpWdsDAO(coaWds)
    val coaRes = coaWdsDAO.getStatus(Uri.unsafeFromString("https://test.com/"), authHeader, AppType.Cromwell)
    val cosStatus = coaRes.unsafeRunSync()
    cosStatus shouldBe true

    val singleWdsDAO = new HttpWdsDAO(singleWds)
    val wdsRes = singleWdsDAO.getStatus(Uri.unsafeFromString("https://test.com/"), authHeader, AppType.Wds)
    val wdsStatus = wdsRes.unsafeRunSync()
    wdsStatus shouldBe true
  }
}
