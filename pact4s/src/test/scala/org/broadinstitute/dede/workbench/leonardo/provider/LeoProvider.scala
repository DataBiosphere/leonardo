package org.broadinstitute.dede.workbench.leonardo.provider

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import cats.effect.IO
import cats.effect.unsafe.implicits.global
import org.broadinstitute.dsde.workbench.leonardo.config.{ContentSecurityPolicyConfig, RefererConfig}
import org.broadinstitute.dsde.workbench.leonardo.http.api.{HttpRoutes, UserInfoDirectives}
import org.broadinstitute.dsde.workbench.leonardo.http.service._
import org.broadinstitute.dsde.workbench.oauth2.OpenIDConnectConfiguration
import org.broadinstitute.dsde.workbench.openTelemetry.OpenTelemetryMetrics
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.time.SpanSugar.convertIntToGrainOfTime
import org.scalatestplus.mockito.MockitoSugar.mock
import org.typelevel.log4cats.StructuredLogger
import org.typelevel.log4cats.slf4j.Slf4jLogger
import pact4s.provider.{PactSource, ProviderInfoBuilder}
import pact4s.scalatest.PactVerifier
import java.nio.file.{Files, Paths}

import java.io.File
import java.lang.Thread.sleep
import scala.concurrent.{ExecutionContext, ExecutionContextExecutor}


class LeoProvider
  extends AnyFlatSpec
  with BeforeAndAfterAll
  with PactVerifier {


implicit val metrics: OpenTelemetryMetrics[IO] = mock[OpenTelemetryMetrics[IO]]
implicit val loggerIO: StructuredLogger[IO] = Slf4jLogger.getLogger[IO]
implicit val ec: ExecutionContextExecutor = ExecutionContext.global
implicit val system: ActorSystem = ActorSystem("leotests")

  val mockOpenIDConnectConfiguration: OpenIDConnectConfiguration = mock[OpenIDConnectConfiguration];
  val mockStatusService: StatusService = mock[StatusService];
  val mockProxyService: ProxyService = mock[ProxyService];
  val mockRuntimeService: RuntimeService[IO] = mock[RuntimeService[IO]];
  val mockDiskService: DiskService[IO] = mock[DiskService[IO]];
  val mockDiskV2Service: DiskV2Service[IO] = mock[DiskV2Service[IO]];
  val mockAppService: AppService[IO] = mock[AppService[IO]];
  val mockRuntimeV2Service: RuntimeV2Service[IO] = mock[RuntimeV2Service[IO]];
  val mockAdminService: AdminService[IO] = mock[AdminService[IO]];
  val mockUserInfoDirectives: UserInfoDirectives = mock[UserInfoDirectives];
  val mockContentSecurityPolicyConfig: ContentSecurityPolicyConfig = mock[ContentSecurityPolicyConfig];
  val refererConfig: RefererConfig = new RefererConfig(Set("*"),true, false)

  val routes =
    new HttpRoutes(
      mockOpenIDConnectConfiguration,
      mockStatusService,
      mockProxyService,
      mockRuntimeService,
      mockDiskService,
      mockDiskV2Service,
      mockAppService,
      mockRuntimeV2Service,
      mockAdminService,
      mockUserInfoDirectives,
      mockContentSecurityPolicyConfig,
      refererConfig
    )


  override def beforeAll(): Unit = {
    val currentDirectory = Paths.get("./pact4s/src/test/resources")

    // List all entries in the current directory
    val entries = Files.list(currentDirectory)

    // Filter out directories
    val directories = entries.filter(Files.isDirectory(_))

    // Print the names of the directories
    directories.forEach(directory => println(directory.getFileName))

    // Close the stream
    entries.close()
    startLeo.unsafeToFuture()
    startLeo.start
    sleep(5000)

  }
  def startLeo: IO[Http.ServerBinding] =
  for {
    binding <- IO
      .fromFuture(IO(Http().newServerAt("localhost",8080).bind(routes.route)))
      .onError { t: Throwable =>
        loggerIO.error(t.toString())
      }
    _ <- IO.fromFuture(IO(binding.whenTerminated))
    _ <- IO(system.terminate())
  } yield binding

  val provider: ProviderInfoBuilder = ProviderInfoBuilder(
    name = "y",
    pactSource = PactSource
      .FileSource(
        Map("x" -> new File("./pact4s/src/test/resources/x-y.json"))
      )).withHost("localhost")
    .withPort(8080)

  it should "Verify pacts" in {
    verifyPacts(
      publishVerificationResults = None,
      providerVerificationOptions = Nil,
      verificationTimeout = Some(10.seconds)
    )
  }
}
