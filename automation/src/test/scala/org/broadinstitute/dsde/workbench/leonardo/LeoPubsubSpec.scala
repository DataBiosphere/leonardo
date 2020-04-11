package org.broadinstitute.dsde.workbench.leonardo

import cats.effect.IO
import fs2.concurrent.InspectableQueue
import org.broadinstitute.dsde.workbench.google2.GooglePublisher
import org.scalatest.time.{Minutes, Span}
import org.scalatest.{DoNotDiscover, FlatSpec}

@DoNotDiscover
class LeoPubsubSpec extends FlatSpec with LeonardoTestUtils {

  "Google publisher" should "be able to auth" in {
    logger.info(s"publisher config is: ${LeonardoConfig.Leonardo.publisherConfig}")

    val publisher = GooglePublisher.resource[IO](LeonardoConfig.Leonardo.publisherConfig)

    publisher
      .use { _ =>
        IO.unit
      }
      .unsafeRunSync()
  }

  it should "publish" in {
    val publisher = GooglePublisher.resource[IO](LeonardoConfig.Leonardo.publisherConfig)
    val queue = InspectableQueue.bounded[IO, String](100).unsafeRunSync()

    publisher
      .use { publisher =>
        (queue.dequeue through publisher.publish).compile.drain
      }
      .unsafeRunAsync(_ => ())

    queue.enqueue1("automation-test-message").unsafeRunSync()

    eventually(timeout(Span(2, Minutes))) {
      val size = queue.getSize.unsafeRunSync()
      size shouldBe 0
    }
  }

}
