package org.broadinstitute.dsde.workbench.leonardo

import cats.effect.IO
import org.scalatest.flatspec.AnyFlatSpecLike
import org.scalatest.matchers.should.Matchers

import scala.concurrent.duration._

class KeyLockSpec extends LeonardoTestSuite with Matchers with AnyFlatSpecLike {
  "KeyLock" should "perform operations using withKeyLock" in {
    val test = KeyLock[IO, String](cache)

    val res =
      for {
        _ <- cache.removeAll

        key1 = "key1"
        key2 = "key2"
        r1 <- test.withKeyLock(key1)(IO(10))
        r2 <- test.withKeyLock(key2)(IO(20))
        r3 <- test.withKeyLock(key1)(IO(30))
      } yield {
        r1 shouldBe 10
        r2 shouldBe 20
        r3 shouldBe 30
      }

    res.timeout(5 seconds).unsafeRunSync()(cats.effect.unsafe.implicits.global)
  }

  it should "block on withKeyLock for the same key" in {
    val test = KeyLock[IO, String](cache)

    val res =
      for {
        _ <- cache.removeAll
        key = "key"
        timeoutErr <- test
          .withKeyLock(key)(
            test.withKeyLock(key)(IO(10))
          )
          .timeout(1 second)
          .attempt
      } yield {
        timeoutErr.isLeft shouldBe true
      }

    res.timeout(5 seconds).unsafeRunSync()(cats.effect.unsafe.implicits.global)
  }
}
