package org.broadinstitute.dsde.workbench.leonardo.drs

import cats.effect.{IO, Resource}
import io.circe.{Decoder, Encoder}
import org.broadinstitute.dsde.workbench.auth.AuthToken
import org.broadinstitute.dsde.workbench.leonardo.LeonardoApiClient.{defaultMediaType}
import org.broadinstitute.dsde.workbench.leonardo.{GPAllocFixtureSpec, LeonardoTestUtils, RestError}
import org.broadinstitute.dsde.workbench.util.ExecutionContexts
import org.http4s.{AuthScheme, Headers, Method, Request, Uri}
import org.http4s.Credentials.Token
import org.http4s.client.middleware.Logger
import org.http4s.client.{Client, blaze}
import org.http4s.headers.Authorization
import org.scalatest.ParallelTestExecution
import org.http4s.circe.CirceEntityEncoder._
import org.http4s._
import org.http4s.circe.CirceEntityDecoder._

import sys.process._


final case class MarthaRequest(url: String) extends AnyVal

final case class MarthaResponse(dos: MarthaDataObject)
final case class MarthaDataObject(urls: List[MarthaUrl])
final case class MarthaUrl(url: String) extends AnyVal

class DrsSpec extends GPAllocFixtureSpec with ParallelTestExecution with LeonardoTestUtils {

  implicit val ronToken: AuthToken = ronAuthToken
  implicit def http4sBody[A](body: A)(implicit encoder: EntityEncoder[IO, A]): EntityBody[IO] =
    encoder.toEntity(body).body

  implicit val marthaPayloadEncoder: Encoder[MarthaRequest] = Encoder.encodeString.contramap(_.url)

  implicit val marthaUrlDecoder: Decoder[MarthaUrl] = Decoder.decodeString.map(MarthaUrl)

  implicit val marthaDataObjectDecoder: Decoder[MarthaDataObject] = Decoder.instance { c =>
    for {
      urls <- c.downField("urls").as[List[MarthaUrl]]
    } yield MarthaDataObject(urls)
  }


  implicit val marthaResponseDecoder: Decoder[MarthaResponse] = Decoder.instance { c =>
    for {
      dos <- c.downField("dos").as[MarthaDataObject]
    } yield MarthaResponse(dos)
  }

  //PUT
  val rawlsUrl = Uri.unsafeFromString("https://rawls.dsde-dev.broadinstitute.org/api/workspaces/general-dev-billing-account/drs-test/disableRequesterPaysForLinkedServiceAccounts")
  //POST
  val marthaUrl = Uri.unsafeFromString("https://us-central1-broad-dsde-dev.cloudfunctions.net/martha_v2")

  val testDrsUrl = ""

  val client: Resource[IO, Client[IO]] = for {
    blockingEc <- ExecutionContexts.cachedThreadPool[IO]
    client <- blaze.BlazeClientBuilder[IO](blockingEc).resource
  } yield Logger[IO](logHeaders = false, logBody = true)(client)

  "should be able to resolve a DRS URL in a requester pays bucket" - {
     val test = client.use { restClient =>
       implicit val authHeader = Authorization(Token(AuthScheme.Bearer, ronCreds.makeAuthToken().value))

       for {
       //make the rawls call to enable requester pays for this user in this project in this workspace
        _ <- restClient
          .expectOr[String](
            Request[IO](
              method = Method.PUT,
              headers = Headers.of(authHeader),
              uri = rawlsUrl,
              body = EmptyBody
            )
          )(resp =>
            resp.bodyText.compile.string
              .flatMap(body => IO.raiseError(RestError(resp.status, body)))
          )
          .void

        //make a martha call to resolve a drs url
        resp <- restClient.expect[MarthaResponse](
          Request[IO](
            method = Method.POST,
            headers = Headers.of(authHeader, defaultMediaType),
            uri = marthaUrl,
            body = MarthaRequest("drs://dg.712C/fa640b0e-9779-452f-99a6-16d833d15bd0")
          )
        )

       _  = resp.dos.urls shouldBe List(MarthaUrl("gs://fc-56ac46ea-efc4-4683-b6d5-6d95bed41c5e/CCDG_13607/Project_CCDG_13607_B01_GRM_WGS.cram.2019-02-06/Sample_HG01131/analysis/HG01131.final.cram.crai"))
       respCode <- IO("gsutil cp -u general-dev-billing-account cp gs://fc-56ac46ea-efc4-4683-b6d5-6d95bed41c5e/CCDG_13607/Project_CCDG_13607_B01_GRM_WGS.cram.2019-02-06/Sample_HG01131/analysis/HG01131.final.cram.crai /tmp".!)
       } yield respCode shouldBe 0

     }

    test.unsafeRunSync()
  }
}
