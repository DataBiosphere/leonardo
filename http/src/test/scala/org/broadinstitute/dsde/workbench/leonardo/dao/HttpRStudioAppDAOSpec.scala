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

class HttpRStudioAppDAOSpec extends AnyFlatSpec with Matchers with LeonardoTestSuite {
  "HttpRStudioAppDAO.getStatus" should "return true if status is ok" in {
    val RStudioAppDAO = new HttpRStudioAppDAO(okRStudioApp)
    val res = RStudioAppDAO.getStatus(Uri.unsafeFromString("https://test.com/rstudio/status"), authHeader)
    res.unsafeRunSync() shouldBe true
  }

  it should "return false if status is not ok" in {
    val RStudioAppDAO = new HttpRStudioAppDAO(notOkRStudioApp)
    val res = RStudioAppDAO.getStatus(Uri.unsafeFromString("https://test.com/rstudio/status"), authHeader)
    res.unsafeRunSync() shouldBe false
  }

  private def okRStudioApp: Client[IO] = {
    val response = "Ca marche !"
    Client.fromHttpApp[IO](
      HttpApp(_ => IO(Response(status = Status.Ok).withEntity(response)))
    )
  }

  private def notOkRStudioApp: Client[IO] = {
    val response = "Oh zut !"
    Client.fromHttpApp[IO](
      HttpApp(_ => IO(Response(status = Status.InternalServerError).withEntity(response)))
    )
  }

  private def authHeader = Authorization(Credentials.Token(AuthScheme.Bearer, "token"))
}
