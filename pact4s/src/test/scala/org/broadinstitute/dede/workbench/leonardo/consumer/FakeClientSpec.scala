package org.broadinstitute.dede.workbench.leonardo.consumer

import au.com.dius.pact.consumer.dsl.{PactDslResponse, PactDslWithProvider}
import au.com.dius.pact.consumer.{ConsumerPactBuilder, PactTestExecutionContext}
import au.com.dius.pact.core.model.RequestResponsePact
import cats.effect.IO
import cats.effect.unsafe.implicits.global
import io.circe.Json
import io.circe.syntax.EncoderOps
import org.broadinstitute.dede.workbench.leonardo.consumer.AuthHelper._
import org.broadinstitute.dede.workbench.leonardo.consumer.PactHelper.buildInteraction
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
  val consumerPactBuilder: ConsumerPactBuilder = ConsumerPactBuilder
    .consumer("leo-consumer")

  val pactProvider: PactDslWithProvider = consumerPactBuilder
    .hasPactWith("fake-provider")

  var pactDslResponse: PactDslResponse = buildInteraction(
    pactProvider,
    state = "resource exists",
    stateParams = Map("id" -> testID, "value" -> 123),
    uponReceiving = "Request to fetch extant resource",
    method = "GET",
    path = s"/resource/$testID",
    requestHeaders = Seq("Authorization" -> mockBearerHeader(MockSamDAO.petSA)),
    status = 200,
    responseHeaders = Seq("Content-type" -> "application/json"),
    Json.obj("id" -> testID.asJson, "value" -> 123.asJson)
  )

  pactDslResponse = buildInteraction(
    pactDslResponse,
    state = "resource does not exist",
    uponReceiving = "Request to fetch missing resource",
    method = "GET",
    path = s"/resource/$missingID",
    requestHeaders = Seq("Authorization" -> mockBearerHeader(MockSamDAO.petSA)),
    status = 404
  )

  pactDslResponse = buildInteraction(
    pactDslResponse,
    uponReceiving = "Request to fetch resource with wrong auth",
    method = "GET",
    path = s"/resource/$testID",
    requestHeaders = Seq("Authorization" -> mockBearerHeader(MockSamDAO.petMI)),
    status = 401
  )

  pactDslResponse = buildInteraction(
    pactDslResponse,
    state = "resource does not exist",
    uponReceiving = "Request to create new resource",
    method = "POST",
    path = "/resource",
    requestHeaders = Seq("Authorization" -> mockBearerHeader(MockSamDAO.petSA)),
    requestBody = newResource,
    status = 204
  )

  pactDslResponse = buildInteraction(
    pactDslResponse,
    state = "resource exists",
    stateParams = Map("id" -> conflictResource.id, "value" -> conflictResource.value),
    uponReceiving = "Request to create resource that already exists",
    method = "POST",
    path = "/resource",
    requestHeaders = Seq("Authorization" -> mockBearerHeader(MockSamDAO.petSA)),
    requestBody = conflictResource,
    status = 409
  )

  override def pact: RequestResponsePact = pactDslResponse.toPact

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
