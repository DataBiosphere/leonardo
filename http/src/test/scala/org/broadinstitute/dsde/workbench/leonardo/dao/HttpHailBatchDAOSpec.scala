package org.broadinstitute.dsde.workbench.leonardo.dao

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import org.broadinstitute.dsde.workbench.leonardo.LeonardoTestSuite
import org.broadinstitute.dsde.workbench.leonardo.TestUtils.appContext
import org.http4s._
import org.http4s.circe.CirceEntityEncoder._
import org.http4s.client.Client
import org.http4s.headers.Authorization
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class HttpHailBatchDAOSpec extends AnyFlatSpec with Matchers with LeonardoTestSuite {
  "HttpHailBatchDAO.getStatus" should "return true if status is ok" in {
    val hailBatchDAO = new HttpHailBatchDAO(okHail)
    val res = hailBatchDAO.getStatus(Uri.unsafeFromString("https://test.com/hail/status"), authHeader)
    res.unsafeRunSync() shouldBe true
  }

  it should "return false if status is not ok" in {
    val hailBatchDAO = new HttpHailBatchDAO(notOkHail)
    val res = hailBatchDAO.getStatus(Uri.unsafeFromString("https://test.com/hail/status"), authHeader)
    res.unsafeRunSync() shouldBe false
  }

  "HttpHailBatchDAO.getDriverStatus" should "return true if status is ok" in {
    val hailBatchDAO = new HttpHailBatchDAO(okHail)
    val res = hailBatchDAO.getDriverStatus(Uri.unsafeFromString("https://test.com/hail/status"), authHeader)
    res.unsafeRunSync() shouldBe true
  }

  it should "return false if status is not ok" in {
    val hailBatchDAO = new HttpHailBatchDAO(notOkHail)
    val res = hailBatchDAO.getDriverStatus(Uri.unsafeFromString("https://test.com/hail/status"), authHeader)
    res.unsafeRunSync() shouldBe false
  }

  private def okHail: Client[IO] = {
    val response = "huzzah"
    Client.fromHttpApp[IO](
      HttpApp(_ => IO(Response(status = Status.Ok).withEntity(response)))
    )
  }

  private def notOkHail: Client[IO] = {
    val response = "alas"
    Client.fromHttpApp[IO](
      HttpApp(_ => IO(Response(status = Status.InternalServerError).withEntity(response)))
    )
  }

  private def authHeader = Authorization(Credentials.Token(AuthScheme.Bearer, "token"))
}
