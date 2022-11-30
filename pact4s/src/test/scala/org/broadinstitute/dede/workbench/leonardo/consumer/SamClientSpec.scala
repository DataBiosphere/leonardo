package org.broadinstitute.dede.workbench.leonardo.consumer

import au.com.dius.pact.consumer.dsl.LambdaDsl.newJsonBody
import au.com.dius.pact.consumer.dsl._
import au.com.dius.pact.consumer.{ConsumerPactBuilder, PactTestExecutionContext}
import au.com.dius.pact.core.model.RequestResponsePact
import cats.effect.IO
import cats.effect.unsafe.implicits._
import io.circe.parser._
import org.broadinstitute.dede.workbench.leonardo.consumer.AuthHelper._
import org.broadinstitute.dede.workbench.leonardo.consumer.PactHelper._
import org.broadinstitute.dsde.workbench.leonardo.JsonCodec._
import org.broadinstitute.dsde.workbench.leonardo.SamResourceId.WorkspaceResourceSamResourceId
import org.broadinstitute.dsde.workbench.leonardo.dao.HttpSamDAO._
import org.broadinstitute.dsde.workbench.leonardo.dao.{ListResourceResponse, MockSamDAO}
import org.broadinstitute.dsde.workbench.leonardo.{SamPolicyName, WorkspaceId}
import org.broadinstitute.dsde.workbench.util.health.Subsystems._
import org.broadinstitute.dsde.workbench.util.health.{StatusCheckResponse, SubsystemStatus}
import org.http4s.Uri
import org.http4s.client.Client
import org.http4s.ember.client.EmberClientBuilder
import org.http4s.headers.Authorization
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import pact4s.scalatest.RequestResponsePactForger

import java.util.UUID

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

  // These fixtures are used for assertions in scala tests
  val subsystems = List(GoogleGroups, GooglePubSub, GoogleIam, Database)
  val okSystemStatus: StatusCheckResponse = StatusCheckResponse(
    ok = true,
    systems = subsystems.map(s => (s, SubsystemStatus(ok = true, messages = None))).toMap
  )

  // We can directly specify the decoded ListResourceResponse[WorkspaceResourceSamResourceId] or
  // use implicit decoder to decode workspaceResourceResponseStr as ListResourceResponse[WorkspaceResourceSamResourceId]
  val workspaceResourceResponse1: ListResourceResponse[WorkspaceResourceSamResourceId] = ListResourceResponse(
    WorkspaceResourceSamResourceId(WorkspaceId(UUID.fromString("cea587e9-9a8e-45b6-b985-9e3803754020"))),
    Set(
      SamPolicyName.Owner,
      SamPolicyName.Other("project-owner")
    )
  )

  val workspaceResourceResponsePlaceholder: ListResourceResponse[WorkspaceResourceSamResourceId] = ListResourceResponse(
    WorkspaceResourceSamResourceId(WorkspaceId(UUID.fromString("00000000-0000-0000-0000-000000000000"))),
    Set()
  )

  val workspaceResourceResponseStr: String =
    """
      |{
      |    "authDomainGroups":
      |    [],
      |    "direct":
      |    {
      |        "actions":
      |        [],
      |        "roles":
      |        [
      |            "project-owner",
      |            "owner"
      |        ]
      |    },
      |    "inherited":
      |    {
      |        "actions":
      |        [],
      |        "roles":
      |        []
      |    },
      |    "missingAuthDomainGroups":
      |    [],
      |    "public":
      |    {
      |        "actions":
      |        [],
      |        "roles":
      |        []
      |    },
      |    "resourceId": "cea587e9-9a8e-45b6-b985-9e3803754020"
      |}
      |""".stripMargin

  // use implicit listResourceResponseDecoder[R] to decode Json string
  val workspaceResourceResponse: ListResourceResponse[WorkspaceResourceSamResourceId] =
    decode[ListResourceResponse[WorkspaceResourceSamResourceId]](workspaceResourceResponseStr)
      .getOrElse(workspaceResourceResponsePlaceholder)

  // --- End of fixtures section

  // ---- Dsl for specifying pacts between consumer and provider
  // Lambda Dsl: required for generating matching rules.
  // Favored over old-style Pact Dsl using PactDslJsonBody.
  val okSystemStatusDsl: DslPart = newJsonBody { o =>
    o.booleanType("ok", true)
    o.`object`("systems",
               s =>
                 for (subsystem <- subsystems)
                   s.`object`(subsystem.value, o => o.booleanType("ok", true))
    )
  }.build()

  val workspaceResourceResponseDsl: DslPart = newJsonBody { o =>
    o.uuid("resourceId", UUID.fromString("cea587e9-9a8e-45b6-b985-9e3803754020"))
    o.`object`(
      "direct",
      s => {
        s.array("actions", _ => Set())
        s.array("roles",
                a => {
                  a.stringType("project-owner")
                  a.stringType("owner")
                }
        )
      }
    )
    o.`object`("inherited",
               s => {
                 s.array("actions", _ => Set())
                 s.array("roles", _ => Set())
               }
    )
    o.`object`("public",
               s => {
                 s.array("actions", _ => Set())
                 s.array("roles", _ => Set())
               }
    )
  }.build()

  val consumerPactBuilder: ConsumerPactBuilder = ConsumerPactBuilder
    .consumer("leo-consumer")

  val pactProvider: PactDslWithProvider = consumerPactBuilder
    .hasPactWith("sam-provider")

  var pactDslResponse: PactDslResponse = buildInteraction(
    pactProvider,
    state = "status is healthy",
    uponReceiving = "Request to status",
    method = "GET",
    path = "/status",
    requestHeaders = Seq("Accept" -> "application/json"),
    status = 200,
    responseHeaders = Seq("Content-type" -> "application/json"),
    okSystemStatusDsl
  )

  pactDslResponse = buildInteraction(
    pactDslResponse,
    state = "runtime resource type",
    uponReceiving = "Request to list runtime resources",
    method = "GET",
    path = "/api/resources/v2/workspace",
    requestHeaders = Seq("Accept" -> "application/json"),
    status = 200,
    responseHeaders = Seq("Content-type" -> "application/json"),
    workspaceResourceResponseDsl
  )

  override val pact: RequestResponsePact = pactDslResponse.toPact

  val client: Client[IO] = EmberClientBuilder.default[IO].build.allocated.unsafeRunSync()._1

  /*
  we should use these tests to ensure that our client class correctly handles responses from the provider - i.e. decoding, error mapping, validation
   */
  it should "get Sam ok status" in {
    new SamClientImpl[IO](client, Uri.unsafeFromString(mockServer.getUrl), mockAuthToken(MockSamDAO.petSA))
      .fetchSystemStatus()
      .attempt
      .unsafeRunSync() shouldBe Right(okSystemStatus)
  }

  it should "fetch authorized workspace resources" in {
    new SamClientImpl[IO](client, Uri.unsafeFromString(mockServer.getUrl), mockAuthToken(MockSamDAO.petSA))
      .fetchResourcePolicies[WorkspaceResourceSamResourceId](Authorization(mockAuthToken(MockSamDAO.petSA)))
      .attempt
      .unsafeRunSync() shouldBe Right(workspaceResourceResponse) // workspaceResourceResponse1 also works
  }
}
