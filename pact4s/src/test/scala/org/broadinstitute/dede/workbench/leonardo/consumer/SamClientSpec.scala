package org.broadinstitute.dede.workbench.leonardo.consumer

import au.com.dius.pact.consumer.dsl.LambdaDsl.newJsonBody
import au.com.dius.pact.consumer.dsl.PactDslJsonBody
import au.com.dius.pact.consumer.{ConsumerPactBuilder, PactTestExecutionContext}
import au.com.dius.pact.core.model.RequestResponsePact
import cats.effect.IO
import cats.effect.unsafe.implicits._
import io.circe.Json
import io.circe.syntax._
import org.broadinstitute.dsde.workbench.leonardo.dao.MockSamDAO
import org.broadinstitute.dsde.workbench.model.WorkbenchEmail
import org.broadinstitute.dsde.workbench.util.health.Subsystems._
import org.broadinstitute.dsde.workbench.util.health.{StatusCheckResponse, SubsystemStatus}
import org.http4s.Credentials.Token
import org.http4s.client.Client
import org.http4s.ember.client.EmberClientBuilder
import org.http4s.{AuthScheme, Credentials, Uri}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import pact4s.circe.implicits._
import pact4s.scalatest.RequestResponsePactForger

class SamClientSpec extends AnyFlatSpec with Matchers with RequestResponsePactForger {
  /*
    we can define the folder that the pact contracts get written to upon completion of this test suite.
  */
  override val pactTestExecutionContext: PactTestExecutionContext =
    new PactTestExecutionContext(
      "./target/pacts"
    )

  // Uncomment this so that mock server will run on specific port (e.g. 9003) instead of dynamically generated port.
  // override val mockProviderConfig: MockProviderConfig = MockProviderConfig.httpConfig("localhost", 9003)
  val testID = "testID"
  val missingID = "missingID"
  val newResource: Resource = Resource("newID", 234)
  val conflictResource: Resource = Resource("conflict", 234)
  val subsystems = List(GoogleGroups, GooglePubSub, GoogleIam, Database)
  //val msgs = Some(List("test"))
  val okSystemStatus: StatusCheckResponse = StatusCheckResponse(
    ok = true,
    systems = subsystems.map(s => (s, SubsystemStatus(ok = true, messages = None))).toMap
  )

  val okSystemStatusDsl = newJsonBody(o => {
    o.booleanType("ok", true)
    o.`object`("systems", s => {
      for (subsystem <- subsystems) {
        s.`object`(subsystem.value, o => { o.booleanType("ok", true) })
      }
    } )
  }).build()

  println("okSystemStatusDsl")
  println(okSystemStatusDsl)

  val okSystemStatusJson = new PactDslJsonBody()
    .booleanType("ok", true)
    .`object`("systems")
      .`object`(GoogleGroups.value)
        .booleanType("ok", true)
        //.minArrayLike("messages", 0, PactDslJsonRootValue.stringType())
      .closeObject()
      .`object`(GooglePubSub.value)
        .booleanType("ok", true)
        //.minArrayLike("messages", 0, PactDslJsonRootValue.stringType())
      .closeObject()
      .`object`(GoogleIam.value)
        .booleanType("ok", true)
        //.minArrayLike("messages", 0, PactDslJsonRootValue.stringType())
      .closeObject()
      .`object`(Database.value)
        .booleanType("ok", true)
        //.minArrayLike("messages", 0, PactDslJsonRootValue.stringType())
      .closeObject()
    .closeObject()

  val pact: RequestResponsePact =
    ConsumerPactBuilder
      .consumer("sam-consumer")
      .hasPactWith("sam-provider")
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
      // -------------------------- GET SUBSYSTEM STATUS --------------------------
      .`given`("status is healthy")
      .uponReceiving("Request to status")
      .method("GET")
      .path("/status")
      .headers("Accept" -> "application/json")
      .willRespondWith()
      .status(200)
      .headers("Content-type" -> "application/json")
      .body(okSystemStatusDsl)
      .toPact

  val client: Client[IO] = EmberClientBuilder.default[IO].build.allocated.unsafeRunSync()._1

  println("okSystemStatusJson")
  println(okSystemStatusJson)

  def mockBearerHeader(workbenchEmail: WorkbenchEmail) = s"Bearer TokenFor$workbenchEmail"

  def mockAuthToken(workbenchEmail: WorkbenchEmail): Token = Credentials.Token(AuthScheme.Bearer, s"TokenFor$workbenchEmail")

  /*
  we should use these tests to ensure that our client class correctly handles responses from the provider - i.e. decoding, error mapping, validation
   */
  it should "handle fetch request for extant resource" in {
    new SamClientImpl[IO](client, Uri.unsafeFromString(mockServer.getUrl), mockAuthToken(MockSamDAO.petSA))
      .fetchResource(testID)
      .unsafeRunSync() shouldBe Some(Resource(testID, 123))
  }

  it should "handle fetch request for missing resource" in {
    new SamClientImpl[IO](client, Uri.unsafeFromString(mockServer.getUrl), mockAuthToken(MockSamDAO.petSA))
      .fetchResource(missingID)
      .unsafeRunSync() shouldBe None
  }

  it should "handle fetch request with incorrect auth" in {
    new SamClientImpl[IO](client, Uri.unsafeFromString(mockServer.getUrl), mockAuthToken(MockSamDAO.petMI))
      .fetchResource(testID)
      .attempt
      .unsafeRunSync() shouldBe Left(InvalidCredentials)
  }

  it should "handle create request for new resource" in {
    new SamClientImpl[IO](client, Uri.unsafeFromString(mockServer.getUrl), mockAuthToken(MockSamDAO.petSA))
      .createResource(newResource)
      .unsafeRunSync()
  }

  it should "handle create request for existing resource" in {
    new SamClientImpl[IO](client, Uri.unsafeFromString(mockServer.getUrl), mockAuthToken(MockSamDAO.petSA))
      .createResource(conflictResource)
      .attempt
      .unsafeRunSync() shouldBe Left(UserAlreadyExists)
  }

  it should "get Sam ok status" in {
    new SamClientImpl[IO](client, Uri.unsafeFromString(mockServer.getUrl), mockAuthToken(MockSamDAO.petSA))
      .fetchSystemStatus()
      .attempt
      .unsafeRunSync() shouldBe Right(Some(okSystemStatus))
  }
}
