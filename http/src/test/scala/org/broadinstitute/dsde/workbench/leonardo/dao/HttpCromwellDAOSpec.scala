package org.broadinstitute.dsde.workbench.leonardo
package dao

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import io.circe.parser._
import org.broadinstitute.dsde.workbench.leonardo.TestUtils.appContext
import org.broadinstitute.dsde.workbench.leonardo.dao.HttpCromwellDAO.statusDecoder
import org.broadinstitute.dsde.workbench.leonardo.db.TestComponent
import org.http4s._
import org.http4s.circe.CirceEntityEncoder._
import org.http4s.client.Client
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.typelevel.ci.CIString

class HttpCromwellDAOSpec extends AnyFlatSpec with Matchers with LeonardoTestSuite with TestComponent {
  "HttpCromwellDAO" should "decode cromwell status endpoint response successfully" in {
    val response =
      """
        |{
        |  "ok": true
        |}
      """.stripMargin

    val res = for {
      json <- parse(response)
      resp <- json.as[CromwellStatusCheckResponse]
    } yield resp

    res shouldBe (Right(CromwellStatusCheckResponse(true)))
  }

  "HttpCromwellDAO.getStatus" should "return false if status is not ok" in {
    val headers = Headers(Header.Raw(CIString("Authorization"), "Bearer token"))

    val response =
      """
        |{
        |  "ok": false
        |}
      """.stripMargin

    val okCrom = Client.fromHttpApp[IO](
      HttpApp(_ => IO.fromEither(parse(response)).flatMap(r => IO(Response(status = Status.Ok).withEntity(r))))
    )

    val cromwellDAO = new HttpCromwellDAO(okCrom)
    val res = cromwellDAO.getStatus(Uri.unsafeFromString("https://test.com/cromwell/status"), headers)
    val status = res.unsafeRunSync()
    status shouldBe false
  }

  "HttpCromwellDAO.getStatus" should "return true if status is ok" in {
    val headers = Headers(Header.Raw(CIString("Authorization"), "Bearer token"))

    val response =
      """
        |{
        |  "ok": true
        |}
      """.stripMargin

    val okCrom = Client.fromHttpApp[IO](
      HttpApp(_ => IO.fromEither(parse(response)).flatMap(r => IO(Response(status = Status.Ok).withEntity(r))))
    )

    val cromwellDAO = new HttpCromwellDAO(okCrom)
    val res = cromwellDAO.getStatus(Uri.unsafeFromString("https://test.com/cromwell/status"), headers)
    val status = res.unsafeRunSync()
    status shouldBe true
  }
}
