package org.broadinstitute.dsde.workbench.leonardo

import cats.effect.IO
import cats.effect.std.Queue
import cats.effect.unsafe.implicits.global
import org.broadinstitute.dsde.workbench.google2.GooglePublisher
import org.scalatest.time.{Minutes, Span}
import org.scalatest.DoNotDiscover
import org.scalatest.flatspec.AnyFlatSpec

@DoNotDiscover
class LeoPubsubSpec extends AnyFlatSpec with LeonardoTestUtils {

  "Google publisher" should "be able to auth" in {
    logger.info(s"publisher config is: ${LeonardoConfig.Leonardo.publisherConfig}")

    val publisher = GooglePublisher.resource[IO](LeonardoConfig.Leonardo.publisherConfig)

    publisher
      .use(_ => IO.unit)
      .unsafeRunSync()(cats.effect.unsafe.implicits.global)
  }

  it should "publish" in {
    val publisher = GooglePublisher.resource[IO](LeonardoConfig.Leonardo.publisherConfig)
    val queue = Queue.bounded[IO, String](100).unsafeRunSync()(cats.effect.unsafe.implicits.global)

    publisher
      .use(publisher => (fs2.Stream.fromQueueUnterminated(queue) through publisher.publish).compile.drain)
      .unsafeRunAsync(_ => ())

    queue.offer("automation-test-message").unsafeRunSync()(cats.effect.unsafe.implicits.global)

    eventually(timeout(Span(2, Minutes))) {
      val size = queue.size.unsafeRunSync()(cats.effect.unsafe.implicits.global)
      size shouldBe 0
    }
  }

}
