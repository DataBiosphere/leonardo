package org.broadinstitute.dsde.workbench.leonardo

import cats.effect.std.Semaphore
import cats.effect.{Deferred, IO}
import com.github.benmanes.caffeine.cache.Caffeine
import fs2.Stream
import org.broadinstitute.dsde.workbench.google2.GKEModels.KubernetesClusterId
import org.broadinstitute.dsde.workbench.openTelemetry.{FakeOpenTelemetryMetricsInterpreter, OpenTelemetryMetrics}
import org.http4s.{AuthScheme, Credentials}
import org.scalatest.matchers.should.Matchers
import org.scalatest.prop.Configuration
import org.scalatest.{Assertion, Assertions}
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks
import org.typelevel.log4cats.StructuredLogger
import org.typelevel.log4cats.slf4j.Slf4jLogger
import scalacache.caffeine.CaffeineCache

import java.util.concurrent.TimeUnit
import scala.concurrent.duration._

trait LeonardoTestSuite extends Matchers {
  implicit val metrics: OpenTelemetryMetrics[IO] = FakeOpenTelemetryMetricsInterpreter
  implicit val loggerIO: StructuredLogger[IO] = Slf4jLogger.getLogger[IO]

  def ioAssertion(test: => IO[Assertion]): Assertion = test.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
  val dummyAuth = org.http4s.headers.Authorization(Credentials.Token(AuthScheme.Bearer, ""))

  val semaphore = Semaphore[IO](10).unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
  val clusterSemaphore = Semaphore[IO](10).unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
  val underlyingCache =
    Caffeine
      .newBuilder()
      .maximumSize(10)
      .expireAfterWrite(60, TimeUnit.SECONDS)
      .recordStats()
      .build[KubernetesClusterId, scalacache.Entry[Semaphore[IO]]]()
  val cache: CaffeineCache[IO, KubernetesClusterId, Semaphore[IO]] =
    CaffeineCache[IO, KubernetesClusterId, Semaphore[IO]](underlyingCache)
  val nodepoolLock = KeyLock[IO, KubernetesClusterId](cache)

  def withInfiniteStream(stream: Stream[IO, Unit], validations: IO[Assertion], maxRetry: Int = 120): IO[Assertion] = {
    val process = Stream.eval(Deferred[IO, Assertion]).flatMap { signalToStop =>
      val accumulator = Accumulator(maxRetry, None)
      val signal = Stream.unfoldEval(accumulator) { acc =>
        if (acc.maxRetry < 0)
          signalToStop
            .complete(Assertions.fail(s"time out after ${maxRetry} retries", acc.throwable.orNull))
            .as(None)
        else
          IO.sleep(1 seconds) >> validations.attempt.flatMap {
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
