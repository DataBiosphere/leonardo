package org.broadinstitute.dsde.workbench.leonardo

import cats.implicits._
import cats.effect.concurrent.{Deferred, Semaphore}
import cats.effect.{Blocker, ContextShift, IO, Timer}
import io.chrisdavenport.log4cats.StructuredLogger
import io.chrisdavenport.log4cats.slf4j.Slf4jLogger
import org.broadinstitute.dsde.workbench.openTelemetry.FakeOpenTelemetryMetricsInterpreter
import org.scalatest.{Assertion, Assertions, Matchers}
import fs2.Stream
import org.scalatest.prop.Configuration
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.global

trait LeonardoTestSuite extends Matchers {
  implicit val metrics = FakeOpenTelemetryMetricsInterpreter
  implicit val testTimer: Timer[IO] = IO.timer(global)
  implicit val cs: ContextShift[IO] = IO.contextShift(global)
  implicit val loggerIO: StructuredLogger[IO] = Slf4jLogger.getLogger[IO]
  implicit val appContext = AppContext.lift[IO](None).unsafeRunSync()

  val blocker = Blocker.liftExecutionContext(global)
  val semaphore = Semaphore[IO](10).unsafeRunSync()

  def withInfiniteStream(stream: Stream[IO, Unit], validations: IO[Assertion]): IO[Assertion] = {
    val process = Stream.eval(Deferred[IO, Assertion]).flatMap { signalToStop =>
      val accumulator = Accumulator(30, None)
      val signal = Stream.unfoldEval(accumulator) { acc =>
        if (acc.maxRetry < 0)
          signalToStop
            .complete(Assertions.fail(s"time out after retries", acc.throwable.getOrElse(null)))
            .as(None)
        else
          testTimer.sleep(1 seconds) >> validations.attempt.flatMap {
            case Left(e)  => IO.pure(Some(((), Accumulator(acc.maxRetry - 1, Some(e)))))
            case Right(a) => signalToStop.complete(a).as(None)
          }
      }
      val p = Stream(stream.interruptWhen(signalToStop.get.attempt.map(_.map(_ => ()))), signal)
        .parJoin(2)
      p ++ Stream.eval(signalToStop.get)
    }
    process.compile.lastOrError.map(_.asInstanceOf[Assertion])
  }
}

final case class Accumulator(maxRetry: Int, throwable: Option[Throwable])

trait PropertyBasedTesting extends ScalaCheckPropertyChecks with Configuration {
  implicit override val generatorDrivenConfig: PropertyCheckConfiguration = PropertyCheckConfiguration(
    minSuccessful = 3
  )
}
