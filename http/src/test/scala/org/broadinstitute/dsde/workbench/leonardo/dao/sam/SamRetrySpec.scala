package org.broadinstitute.dsde.workbench.leonardo.dao.sam

import cats.effect.unsafe.implicits.global
import cats.effect.{IO, Ref}
import cats.syntax.all._
import org.broadinstitute.dsde.workbench.RetryConfig
import org.broadinstitute.dsde.workbench.leonardo.LeonardoTestSuite
import org.broadinstitute.dsde.workbench.leonardo.TestUtils.appContext
import org.scalatest.flatspec.AnyFlatSpec

import scala.concurrent.duration._

class SamRetrySpec extends AnyFlatSpec with LeonardoTestSuite {
  val testRetryConfig = RetryConfig(100 millis, identity, 5, _.isInstanceOf[TestException])
  val counter: IO[Ref[IO, Int]] = Ref[IO].of(0)
  def incrementCounter(counter: Ref[IO, Int]): IO[Int] = counter.updateAndGet(_ + 1)

  "SamRetry" should "not retry successes" in {
    val test = for {
      c <- counter
      lastResult <- SamRetry.retry(testRetryConfig)(incrementCounter(c), "increment")
      tries <- c.get
    } yield {
      // Counter should have been incremented once
      lastResult shouldBe 1
      tries shouldBe 1
    }
    test.unsafeRunSync()
  }

  it should "retry retryable exceptions" in {
    val test = for {
      c <- counter
      exception = TestException("api error")
      alwaysFail = incrementCounter(c) >> IO.raiseError(exception)
      lastResult <- SamRetry.retry(testRetryConfig)(alwaysFail, "increment").attempt
      tries <- c.get
    } yield {
      // Counter should have been incremented 5 times and result should be the TestException
      lastResult shouldBe Left(exception)
      tries shouldBe 5
    }
    test.unsafeRunSync()
  }

  it should "retry for n attempts" in {
    val test = for {
      c <- counter
      exception = TestException("api error")
      failTwice = incrementCounter(c).flatMap(n => IO.raiseWhen(n < 2)(exception).as(n))
      lastResult <- SamRetry.retry(testRetryConfig)(failTwice, "increment").attempt
      tries <- c.get
    } yield {
      // Counter should have been incremented twice
      lastResult shouldBe Right(2)
      tries shouldBe 2
    }
    test.unsafeRunSync()
  }

  it should "not retry non-retryable exceptions" in {
    val test = for {
      c <- counter
      exception = new RuntimeException("runtime error")
      alwaysFail = incrementCounter(c) >> IO.raiseError(exception)
      lastResult <- SamRetry.retry(testRetryConfig)(alwaysFail, "increment").attempt
      tries <- c.get
    } yield {
      // Counter should have been incremented once and result should be the RuntimeException
      lastResult.isLeft shouldBe true
      lastResult.leftMap(_.getMessage) shouldBe Left(exception.getMessage)
      tries shouldBe 1
    }
    test.unsafeRunSync()
  }
}

case class TestException(message: String) extends Exception(message)
