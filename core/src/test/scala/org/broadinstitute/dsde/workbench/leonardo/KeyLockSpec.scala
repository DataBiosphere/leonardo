package org.broadinstitute.dsde.workbench.leonardo

import cats.effect.IO
import org.scalatest.flatspec.AnyFlatSpecLike
import org.scalatest.matchers.should.Matchers

import scala.concurrent.duration._

class KeyLockSpec extends LeonardoTestSuite with Matchers with AnyFlatSpecLike {

  "Lock" should "block on acquire/release" in {
    val res = for {
      test <- Lock[IO]
      // try to acquire the lock twice
      _ <- test.acquire
      f <- test.acquire.start
      // the second acquire should block
      timeoutErr <- f.join.timeout(1 second).attempt
      // release the lock, the second acquire should now succeed
      _ <- test.release
      _ <- f.join
      _ <- test.release
    } yield {
      timeoutErr.isLeft shouldBe true
    }

    res.timeout(5 seconds).unsafeRunSync()
  }

  it should "perform operations using withLock" in {
    val res = for {
      test <- Lock[IO]
      r1 <- test.withLock(IO(10))
      r2 <- test.withLock(IO(20))
    } yield {
      r1 shouldBe 10
      r2 shouldBe 20
    }

    res.timeout(5 seconds).unsafeRunSync()
  }

  "KeyLock" should "block on acquire/release for the same key" in {
    val res = for {
      test <- KeyLock[IO, String](1 minute, 10, blocker)
      key = "key"
      // try to acquire the lock twice for the same key
      _ <- test.acquire(key)
      f <- test.acquire(key).start
      // the second acquire should block
      timeoutErr <- f.join.timeout(1 second).attempt
      // release the lock, the second acquire should now succeed
      _ <- test.release(key)
      _ <- f.join
      _ <- test.release(key)
    } yield {
      timeoutErr.isLeft shouldBe true
    }

    res.timeout(5 seconds).unsafeRunSync()
  }

  it should "not block on acquire/release for different keys" in {
    val res = for {
      test <- KeyLock[IO, String](1 minute, 10, blocker)
      key1 = "key1"
      key2 = "key2"
      // try to acquire the lock twice for different keys, it should not block
      _ <- test.acquire(key1)
      _ <- test.acquire(key2)
      r1 <- IO(10)
      // release the locks
      _ <- test.release(key1)
      _ <- test.release(key2)
    } yield {
      r1 shouldBe 10
    }

    res.timeout(5 seconds).unsafeRunSync()
  }

  it should "perform operations using withKeyLock" in {
    val res = for {
      test <- KeyLock[IO, String](1 minute, 10, blocker)
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

    res.timeout(5 seconds).unsafeRunSync()
  }
}
