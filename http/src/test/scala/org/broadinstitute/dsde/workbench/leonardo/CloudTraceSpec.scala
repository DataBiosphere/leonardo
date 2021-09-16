package org.broadinstitute.dsde.workbench.leonardo

import java.nio.file.Paths
import akka.http.scaladsl.client.RequestBuilding
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.testkit.ScalatestRouteTest
import cats.effect.IO
import cats.effect.unsafe.implicits.global
import io.opencensus.scala.akka.http.TracingDirective.traceRequestForService
import org.broadinstitute.dsde.workbench.leonardo.http.serviceData
import org.broadinstitute.dsde.workbench.openTelemetry.OpenTelemetryMetrics
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

// This test is useful if we try to manually test cloud tracing. But it's ignored in CI
class CloudTraceSpec
    extends AnyFlatSpec
    with ScalatestRouteTest
    with RequestBuilding
    with Matchers
    with LeonardoTestSuite {

  it should "work" ignore {
    val route = traceRequestForService(serviceData) { span =>
      val action = IO {
        span.addAnnotation("testing")
        span.end()
        "Traced span with context: " + span.getContext
      }.unsafeToFuture()
      complete(action)
    }

    val credentialFilePath = Paths.get("Your SA Path")
    val res = OpenTelemetryMetrics
      .registerTracing[IO](credentialFilePath)
      .use { _ =>
        val req = Post("/route")
        IO {
          req ~> route ~> check {
            1 shouldBe 1
          }
        }
      }
    res.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)

    1 shouldBe (1)
  }
}
