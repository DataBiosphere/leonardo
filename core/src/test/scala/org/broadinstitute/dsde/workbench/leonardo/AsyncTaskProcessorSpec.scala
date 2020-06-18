package org.broadinstitute.dsde.workbench.leonardo

import java.time.Instant

import cats.effect.IO
import cats.effect.concurrent.Deferred
import fs2.Stream
import fs2.concurrent.InspectableQueue
import org.broadinstitute.dsde.workbench.leonardo.AsyncTaskProcessor.{Config, Task}

import scala.concurrent.duration._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class AsyncTaskProcessorSpec extends AnyFlatSpec with Matchers with LeonardoTestSuite {
  val config = Config(20, 5)
  val queue = InspectableQueue.bounded[IO, Task[IO]](config.queueBound).unsafeRunSync()
  val asyncTaskProcessor = new AsyncTaskProcessor[IO](
    config,
    queue
  )

  ignore should "execute tasks concurrently" in {
    val start = Instant.now()
    val res = Stream.eval(Deferred[IO, Unit]).flatMap { signalToStop =>
      val traceId = appContext.ask.unsafeRunSync().traceId

      def io(x: Int): IO[Unit] =
        for {
          _ <- if (x == 1 || x == 4) IO.sleep(10 seconds) else IO.sleep(15 seconds)
// Leaving the commented out code here since it's easier to see what's going on with it
//          _ <- IO(println(s"executing ${x} " + Instant.now()))
          size <- queue.getSize
//          _ <- if (size == 0)
//            signalToStop.complete(())
//          else IO.unit
        } yield ()

      val tasks = Stream
        .emits(1 to 10)
        .covary[IO]
        .map(x => Task(traceId, io(x), None, Instant.now()))

      val stream = tasks.through(queue.enqueue) ++ asyncTaskProcessor.process
      stream.interruptWhen(signalToStop.get.attempt.map(_.map(_ => ())))
    }

    res.compile.drain.timeout(1 minutes).unsafeRunSync()
    val end = Instant.now()
    // If tasks are executed sequentially, then each sleep takes 2 seconds, which will result int at least 20 seconds latency
    // stream terminates where queue becomes empty, but queue becomes empty before all items are processed,
    // hence initialize 15 items in the original queue
    (end.toEpochMilli - start.toEpochMilli < 5000) shouldBe (true)
  }
}
