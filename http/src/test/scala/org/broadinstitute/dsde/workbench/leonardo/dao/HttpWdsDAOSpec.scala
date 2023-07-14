package org.broadinstitute.dsde.workbench.leonardo.dao

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import io.circe.parser._
import org.broadinstitute.dsde.workbench.leonardo.LeonardoTestSuite
import org.broadinstitute.dsde.workbench.leonardo.TestUtils.appContext
import org.broadinstitute.dsde.workbench.leonardo.dao.HttpWdsDAO.statusDecoder
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
    val res = wdsDAO.getStatus(Uri.unsafeFromString("https://test.com/wds/status"), authHeader)
    val status = res.unsafeRunSync()
    status shouldBe false
  }
}
