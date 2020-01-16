package org.broadinstitute.dsde.workbench.leonardo

import cats.effect.{Blocker, ContextShift, IO, Timer}
import io.chrisdavenport.log4cats.Logger
import io.chrisdavenport.log4cats.slf4j.Slf4jLogger
import org.broadinstitute.dsde.workbench.newrelic.mock.FakeNewRelicMetricsInterpreter
import org.scalatest.Matchers
import scala.concurrent.ExecutionContext.global

trait LeonardoTestSuite extends Matchers {
  implicit val metrics = FakeNewRelicMetricsInterpreter
  implicit val timer: Timer[IO] = IO.timer(global)
  implicit val cs: ContextShift[IO] = IO.contextShift(global)
  implicit val loggerIO: Logger[IO] = Slf4jLogger.getLogger[IO]
  val blocker = Blocker.liftExecutionContext(global)
}
