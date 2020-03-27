package org.broadinstitute.dsde.workbench.leonardo

import java.util.UUID

import cats.effect.concurrent.Semaphore
import cats.effect.{Blocker, ContextShift, IO, Timer}
import cats.mtl.ApplicativeAsk
import io.chrisdavenport.log4cats.StructuredLogger
import io.chrisdavenport.log4cats.slf4j.Slf4jLogger
import org.broadinstitute.dsde.workbench.model.TraceId
import org.broadinstitute.dsde.workbench.newrelic.mock.FakeNewRelicMetricsInterpreter
import org.broadinstitute.dsde.workbench.openTelemetry.FakeOpenTelemetryMetricsInterpreter
import org.scalatest.Matchers

import scala.concurrent.ExecutionContext.global

trait LeonardoTestSuite extends Matchers {
  implicit val metrics = FakeNewRelicMetricsInterpreter
  implicit val openTelemetry = FakeOpenTelemetryMetricsInterpreter
  implicit val timer: Timer[IO] = IO.timer(global)
  implicit val cs: ContextShift[IO] = IO.contextShift(global)
  implicit val loggerIO: StructuredLogger[IO] = Slf4jLogger.getLogger[IO]
  implicit val traceId = ApplicativeAsk.const[IO, TraceId](TraceId(UUID.randomUUID())) //we don't care much about traceId in unit tests, hence providing a constant UUID here

  val blocker = Blocker.liftExecutionContext(global)
  val semaphore = Semaphore[IO](10).unsafeRunSync()
}
