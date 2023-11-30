package org.broadinstitute.dede.workbench.leonardo.provider

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.testkit.ScalatestRouteTest
import cats.effect.IO
import cats.effect.unsafe.implicits.global
import org.broadinstitute.dsde.workbench.leonardo.CommonTestData._
import org.broadinstitute.dsde.workbench.leonardo.LeonardoTestSuite
import org.broadinstitute.dsde.workbench.leonardo.db.TestComponent
import org.broadinstitute.dsde.workbench.leonardo.http.api.{HttpRoutes, TestLeoRoutes}
import org.broadinstitute.dsde.workbench.leonardo.http.service._
import org.broadinstitute.dsde.workbench.openTelemetry.{FakeOpenTelemetryMetricsInterpreter, OpenTelemetryMetrics}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.time.SpanSugar.convertIntToGrainOfTime
import org.scalatestplus.mockito.MockitoSugar
import org.typelevel.log4cats.StructuredLogger
import org.typelevel.log4cats.slf4j.Slf4jLogger
import pact4s.provider.{PactSource, ProviderInfoBuilder}
import pact4s.scalatest.PactVerifier

import java.io.File
import java.lang.Thread.sleep
import scala.concurrent.{ExecutionContext, ExecutionContextExecutor}


class LeoProvider
  extends AnyFlatSpec
  with ScalatestRouteTest
  with PactVerifier
    with Matchers
    with ScalaFutures
    with LeonardoTestSuite
    with TestComponent
    with MockitoSugar
  with TestLeoRoutes {


//override implicit val metrics: OpenTelemetryMetrics[IO] = FakeOpenTelemetryMetricsInterpreter
//override implicit val loggerIO: StructuredLogger[IO] = Slf4jLogger.getLogger[IO]
implicit val ec: ExecutionContextExecutor = ExecutionContext.global
override implicit val system: ActorSystem = ActorSystem("leotests")
  val routes =
    new HttpRoutes(
      openIdConnectionConfiguration,
      statusService,
      proxyService,
      MockRuntimeServiceInterp,
      MockDiskServiceInterp,
      MockDiskV2ServiceInterp,
      MockAppService,
      new MockRuntimeV2Interp,
      MockAdminServiceInterp,
      timedUserInfoDirectives,
      contentSecurityPolicy,
      refererConfig
    )

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
        Map("x" -> new File("./example/resources/pacts/x-y.json"))
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
