package org.broadinstitute.dsde.workbench.leonardo.dao

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import io.circe.parser._
import org.broadinstitute.dsde.workbench.leonardo.LeonardoTestSuite
import org.broadinstitute.dsde.workbench.leonardo.TestUtils.appContext
import org.broadinstitute.dsde.workbench.leonardo.dao.HttpCbasDAO.statusDecoder
import org.http4s._
import org.http4s.circe.CirceEntityEncoder._
import org.http4s.client.Client
import org.http4s.headers.Authorization
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class HttpCbasDAOSpec extends AnyFlatSpec with Matchers with LeonardoTestSuite {
  "HttpCbasDAO" should "decode cbas status endpoint response successfully" in {
    val response =
      """
        |{
        |  "ok": true
        |}
      """.stripMargin

    val res = for {
      json <- parse(response)
      resp <- json.as[CbasStatusCheckResponse]
    } yield resp

    res shouldBe (Right(CbasStatusCheckResponse(true)))
  }

  "HttpCbasDAO.getStatus" should "return false if status is not ok" in {
    val authHeader = Authorization(Credentials.Token(AuthScheme.Bearer, "token"))

    val response =
      """
        |{
        |  "ok": false
        |}
      """.stripMargin

    val okCbas = Client.fromHttpApp[IO](
      HttpApp(_ => IO.fromEither(parse(response)).flatMap(r => IO(Response(status = Status.Ok).withEntity(r))))
    )

    val cbasDAO = new HttpCbasDAO(okCbas)
    val res = cbasDAO.getStatus(Uri.unsafeFromString("https://test.com/cbas/status"), authHeader)
    val status = res.unsafeRunSync()
    status shouldBe false
  }

  "HttpCbasDAO.getStatus" should "return true if status is ok" in {
    val authHeader = Authorization(Credentials.Token(AuthScheme.Bearer, "token"))

    val response =
      """
        |{
        |  "ok": true
        |}
      """.stripMargin

    val okCbas = Client.fromHttpApp[IO](
      HttpApp(_ => IO.fromEither(parse(response)).flatMap(r => IO(Response(status = Status.Ok).withEntity(r))))
    )

    val cbasDAO = new HttpCbasDAO(okCbas)
    val res = cbasDAO.getStatus(Uri.unsafeFromString("https://test.com/cbas/status"), authHeader)
    val status = res.unsafeRunSync()
    status shouldBe true
  }
}
