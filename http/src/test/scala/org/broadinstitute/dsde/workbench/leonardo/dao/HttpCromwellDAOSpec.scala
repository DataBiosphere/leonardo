package org.broadinstitute.dsde.workbench.leonardo
package dao

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import io.circe.parser._
import org.broadinstitute.dsde.workbench.azure.RelayNamespace
import org.broadinstitute.dsde.workbench.leonardo.TestUtils.appContext
import org.broadinstitute.dsde.workbench.leonardo.dao.HttpCromwellDAO.statusDecoder
import org.broadinstitute.dsde.workbench.leonardo.db.TestComponent
import org.http4s._
import org.http4s.circe.CirceEntityEncoder._
import org.http4s.client.Client
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.mockito.MockitoSugar.mock
import org.typelevel.ci.CIString
import org.typelevel.log4cats.slf4j.Slf4jLogger

class HttpCromwellDAOSpec extends AnyFlatSpec with Matchers with LeonardoTestSuite with TestComponent {

  implicit def unsafeLogger = Slf4jLogger.getLogger[IO]

  implicit val ec: scala.concurrent.ExecutionContext = scala.concurrent.ExecutionContext.global

  val mockSamDao = mock[SamDAO[IO]]

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

    val okSam = Client.fromHttpApp[IO](
      HttpApp(_ => IO.fromEither(parse(response)).flatMap(r => IO(Response(status = Status.Ok).withEntity(r))))
    )

    val cromwellDAO = new HttpCromwellDAO(okSam, mockSamDao)
    val res = cromwellDAO.getStatus(RelayNamespace("test-namespace"), headers)
    res.map(r => r shouldBe CromwellStatusCheckResponse(false)).unsafeRunSync()
  }

  "HttpCromwellDAO.getStatus" should "return true if status is ok" in {
    val headers = Headers(Header.Raw(CIString("Authorization"), "Bearer token"))

    val response =
      """
        |{
        |  "ok": true
        |}
      """.stripMargin

    val okSam = Client.fromHttpApp[IO](
      HttpApp(_ => IO.fromEither(parse(response)).flatMap(r => IO(Response(status = Status.Ok).withEntity(r))))
    )

    val cromwellDAO = new HttpCromwellDAO(okSam, mockSamDao)
    val res = cromwellDAO.getStatus(RelayNamespace("test-namespace"), headers)
    res.map(r => r shouldBe CromwellStatusCheckResponse(true)).unsafeRunSync()
  }
}
