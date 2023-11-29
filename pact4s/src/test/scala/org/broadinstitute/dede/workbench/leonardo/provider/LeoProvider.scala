package org.broadinstitute.dede.workbench.leonardo.provider

import akka.http.scaladsl.Http
import akka.http.scaladsl.testkit.ScalatestRouteTest
import cats.effect.IO
import cats.effect.unsafe.implicits._
import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.dsde.workbench.leonardo.CommonTestData._
import org.broadinstitute.dsde.workbench.leonardo._
import org.broadinstitute.dsde.workbench.leonardo.http.api.{HttpRoutes, TestLeoRoutes}
import org.broadinstitute.dsde.workbench.leonardo.http.service._
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import pact4s.provider.{PactSource, ProviderInfoBuilder}
import pact4s.scalatest.PactVerifier

import java.io.File
import java.lang.Thread.sleep
import scala.concurrent.duration.DurationInt

class ScalaTestVerifyPacts
  extends AnyFlatSpec
  with ScalatestRouteTest
  with BeforeAndAfterAll
  with PactVerifier
  with LazyLogging
  with TestLeoRoutes
  with LeonardoTestSuite {


  override def beforeAll(): Unit = {
    startLeo.unsafeToFuture()
    startLeo.start
    sleep(5000)
  }

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
        IO(logger.error("FATAL - failure starting http server", t)) *> IO.raiseError(t)
      }
    _ <- IO.fromFuture(IO(binding.whenTerminated))
    _ <- IO(system.terminate())
  } yield binding

  val provider: ProviderInfoBuilder = ProviderInfoBuilder(
    name = "sam",
    pactSource = PactSource
      .FileSource(
        Map("munit-consumer" -> new File("./example/resources/pacts/munit-consumer-munit-provider.json"))
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
