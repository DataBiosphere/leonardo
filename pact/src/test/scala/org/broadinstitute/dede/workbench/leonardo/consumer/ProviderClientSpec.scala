package org.broadinstitute.dede.workbench.leonardo.consumer

import com.itv.scalapact.model.ScalaPactDescription
import com.itv.scalapact.{ScalaPactMockConfig, ScalaPactMockServer}
import org.json4s.DefaultFormats
import org.json4s.native.Serialization._
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

class ProviderClientSpec extends AnyFunSpec with Matchers with BeforeAndAfterAll {
  // The import contains two things:
  // 1. The consumer test DSL/Builder
  // 2. Helper implicits, for instance, values will automatically be converted
  //    to Option types where the DSL requires it.

  import com.itv.scalapact.ScalaPactForger._

  // Import the json and http libraries specified in the build.sbt file
  import com.itv.scalapact.circe14._
  import com.itv.scalapact.http4s23._

  implicit val formats: DefaultFormats.type = DefaultFormats
  lazy val server: ScalaPactMockServer = pact.startServer()
  lazy val config: ScalaPactMockConfig = server.config
  val CONSUMER = "scala-pact-consumer"
  val PROVIDER = "scala-pact-provider"
  // Fixtures
  val people = List("Bob", "Fred", "Harry")
  val systems = List("GoogleGroups", "GooglePubSub", "GoogleIam", "Database")
  val body: String = write(
    Results(
      count = 3,
      results = people
    )
  )
  // Map(
  //        "GoogleGroups" -> OK(ok = true),
  //        "GooglePubSub" -> OK(ok = true),
  //        "GoogleIam" -> OK(ok = true),
  //        "Database" -> OK(ok = true)
  //      )
  val status: String = write(
    Status(
      ok = true,
      systems = systems.map(s => (s, OK(ok = true))).toMap
    )
  )
  // Forge all pacts up front
  val pact: ScalaPactDescription = forgePact
    .between(CONSUMER)
    .and(PROVIDER)
    .addInteraction(
      interaction
        .description("Fetching results")
        .provided("Results: Bob, Fred, Harry")
        .uponReceiving("/results")
        .willRespondWith(200, Map("Pact" -> "modifiedRequest"), body)
    )
    .addInteraction(
      interaction
        .description("Fetching least secure auth token ever")
        .uponReceiving(
          method = GET,
          path = "/auth_token",
          query = None,
          headers = Map("Accept" -> "application/json", "Name" -> "Bob"),
          body = None,
          matchingRules = // When stubbing (during this test or externally), we don't mind
            // what the name is, as long as it only contains letters.
            headerRegexRule("Name", "^([a-zA-Z]+)$")
        )
        .willRespondWith(
          status = 202,
          headers = Map("Content-Type" -> "application/json; charset=UTF-8"),
          body = Some("""{"token":"abcABC123"}"""),
          matchingRules = // When verifying externally, we don't mind what is in the token
            // as long as it contains a token field with an alphanumeric
            // value
            bodyRegexRule("token", "^([a-zA-Z0-9]+)$")
        )
    )
    .addInteraction(
      interaction
        .description("Fetching status")
        .uponReceiving(
          method = GET,
          path = "/status",
          query = None,
          headers = Map("Accept" -> "application/json")
        )
        .willRespondWith(
          status = 200,
          headers = Map("Content-Type" -> "application/json; charset=UTF-8"),
          body = Option(status),
          matchingRules = headerRegexRule("Content-Type", "application/json")
            ~> bodyTypeRule("ok")
            ~> bodyRegexRule("ok", "true")
            ~> bodyTypeRule("systems")
            ~> bodyTypeRule("systems.GoogleGroups")
            ~> bodyTypeRule("systems.GoogleGroups.ok")
            ~> bodyRegexRule("systems.GoogleGroups.ok", "true")
            ~> bodyTypeRule("systems.GooglePubSub")
            ~> bodyTypeRule("systems.GooglePubSub.ok")
            ~> bodyRegexRule("systems.GooglePubSub.ok", "true")
            ~> bodyTypeRule("systems.GoogleIam")
            ~> bodyTypeRule("systems.GoogleIam.ok")
            ~> bodyRegexRule("systems.GoogleIam.ok", "true")
        )
    )

  override def beforeAll(): Unit = {
    // Initialize the Pact stub server prior to tests executing.
    val _ = server
    ()
  }

  override def afterAll(): Unit =
    // Shut down the stub server when tests are finished.
    server.stop()

  describe("Connecting to the Provider service") {
    it("should be able to fetch results") {
      val results = ProviderClient.fetchResults(config.baseUrl)
      results.isDefined shouldEqual true
      results.get.count shouldEqual 3
      results.get.results.forall(p => people.contains(p)) shouldEqual true
    }

    it("should be able to get an auth token") {
      val token = ProviderClient.fetchAuthToken(config.host, config.port, "Sally")
      token.isDefined shouldEqual true
      token.get.token shouldEqual "abcABC123"
    }

    it("should be able to fetch status") {
      val s = ProviderClient.fetchStatus(config.baseUrl)
      s.isDefined shouldEqual true
      s.get.ok shouldEqual true
      val keys = List("GoogleGroups", "GooglePubSub", "GoogleIam", "Database")
      for (key <- keys) {
        s.get.systems.contains(key) shouldEqual true
        if (s.get.systems.contains(key)) {
          s.get.systems(key).ok shouldEqual true
        }
      }
    }
  }
}
