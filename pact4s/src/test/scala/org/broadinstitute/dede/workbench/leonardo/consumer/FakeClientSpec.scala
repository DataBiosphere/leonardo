package org.broadinstitute.dede.workbench.leonardo.consumer

import au.com.dius.pact.consumer.dsl.PactDslResponse
import au.com.dius.pact.consumer.{ConsumerPactBuilder, PactTestExecutionContext}
import au.com.dius.pact.core.model.RequestResponsePact
import cats.effect.IO
import cats.effect.unsafe.implicits.global
import io.circe.Json
import io.circe.syntax.EncoderOps
import org.broadinstitute.dede.workbench.leonardo.consumer.AuthHelper._
import org.broadinstitute.dsde.workbench.leonardo.dao.MockSamDAO
import org.http4s.Uri
import org.http4s.client.Client
import org.http4s.ember.client.EmberClientBuilder
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import pact4s.circe.implicits._
import pact4s.scalatest.RequestResponsePactForger

class FakeClientSpec extends AnyFlatSpec with Matchers with RequestResponsePactForger {

  /*
  we can define the folder that the pact contracts get written to upon completion of this test suite.
   */
  override val pactTestExecutionContext: PactTestExecutionContext =
    new PactTestExecutionContext(
      "./target/pacts"
    )

  val testID = "testID"
  val missingID = "missingID"
  val newResource: Resource = Resource("newID", 234)
  val conflictResource: Resource = Resource("conflict", 234)

  // Build the pact between consumer and provider
  val pactBuilder: PactDslResponse =
    ConsumerPactBuilder
      .consumer("leo-consumer")
      .hasPactWith("fake-provider")
      // -------------------------- FETCH RESOURCE --------------------------
      .`given`(
        "resource exists", // this is a state identifier that is passed to the provider
        Map("id" -> testID, "value" -> 123) // we can use parameters to specify details about the provider state
      )
      .uponReceiving("Request to fetch extant resource")
      .method("GET")
      .path(s"/resource/$testID")
      .headers("Authorization" -> mockBearerHeader(MockSamDAO.petSA))
      .willRespondWith()
      .status(200)
      .body(
        Json.obj("id" -> testID.asJson, "value" -> 123.asJson)
      ) // can use circe json directly for both request and response bodies with `import pact4s.circe.implicits._`
      .`given`("resource does not exist")
      .uponReceiving("Request to fetch missing resource")
      .method("GET")
      .path(s"/resource/$missingID")
      .headers("Authorization" -> mockBearerHeader(MockSamDAO.petSA))
      .willRespondWith()
      .status(404)
      .uponReceiving("Request to fetch resource with wrong auth")
      .method("GET")
      .path(s"/resource/$testID")
      .headers("Authorization" -> mockBearerHeader(MockSamDAO.petMI))
      .willRespondWith()
      .status(401)
      // -------------------------- CREATE RESOURCE --------------------------
      .`given`("resource does not exist")
      .uponReceiving("Request to create new resource")
      .method("POST")
      .path("/resource")
      .headers("Authorization" -> mockBearerHeader(MockSamDAO.petSA))
      .body(newResource) // can use classes directly in the body if they are encodable
      .willRespondWith()
      .status(204)
      .`given`(
        "resource exists",
        Map("id" -> conflictResource.id, "value" -> conflictResource.value)
      ) // notice we're using the same state, but with different parameters
      .uponReceiving("Request to create resource that already exists")
      .method("POST")
      .path("/resource")
      .headers("Authorization" -> mockBearerHeader(MockSamDAO.petSA))
      .body(conflictResource)
      .willRespondWith()
      .status(409)

  override def pact: RequestResponsePact = pactBuilder.toPact

  val client: Client[IO] = EmberClientBuilder.default[IO].build.allocated.unsafeRunSync()._1

  /*
  we should use these tests to ensure that our client class correctly handles responses from the provider - i.e. decoding, error mapping, validation
   */
  it should "handle fetch request for extant resource" in {
    new FakeClientImpl[IO](client, Uri.unsafeFromString(mockServer.getUrl), mockAuthToken(MockSamDAO.petSA))
      .fetchResource(testID)
      .unsafeRunSync() shouldBe Some(Resource(testID, 123))
  }

  it should "handle fetch request for missing resource" in {
    new FakeClientImpl[IO](client, Uri.unsafeFromString(mockServer.getUrl), mockAuthToken(MockSamDAO.petSA))
      .fetchResource(missingID)
      .unsafeRunSync() shouldBe None
  }

  it should "handle fetch request with incorrect auth" in {
    new FakeClientImpl[IO](client, Uri.unsafeFromString(mockServer.getUrl), mockAuthToken(MockSamDAO.petMI))
      .fetchResource(testID)
      .attempt
      .unsafeRunSync() shouldBe Left(InvalidCredentials)
  }

  it should "handle create request for new resource" in {
    new FakeClientImpl[IO](client, Uri.unsafeFromString(mockServer.getUrl), mockAuthToken(MockSamDAO.petSA))
      .createResource(newResource)
      .unsafeRunSync()
  }

  it should "handle create request for existing resource" in {
    new FakeClientImpl[IO](client, Uri.unsafeFromString(mockServer.getUrl), mockAuthToken(MockSamDAO.petSA))
      .createResource(conflictResource)
      .attempt
      .unsafeRunSync() shouldBe Left(UserAlreadyExists)
  }
}
