package org.broadinstitute.dsde.workbench.leonardo

import cats.effect.IO
import fs2.concurrent.InspectableQueue
import org.broadinstitute.dsde.workbench.google2.GooglePublisher
import org.broadinstitute.dsde.workbench.service.util.Tags
import org.scalatest.time.{Minutes, Span}
import org.scalatest.BeforeAndAfterAll

//@DoNotDiscover
class LeoPubsubSpec extends ClusterFixtureSpec with BeforeAndAfterAll with LeonardoTestUtils {

  "Google publisher should be able to auth" taggedAs Tags.SmokeTest in { clusterFixture =>
    val publisher = GooglePublisher.resource[IO, String](LeonardoConfig.Leonardo.publisherConfig)
    publisher.use {
      _ => IO.unit
    }
  }

  "Google publisher should publish" in { _ =>
    val publisher =  GooglePublisher.resource[IO, String](LeonardoConfig.Leonardo.publisherConfig)
    val queue =  InspectableQueue.bounded[IO, String](100).unsafeRunSync()

    publisher.use { publisher =>
      (queue.dequeue through publisher.publish)
        .compile
        .drain
    }
      .unsafeRunAsync(_ => ())

    queue.enqueue1("automation-test-message").unsafeRunSync()

    eventually(timeout(Span(2, Minutes))) {
      val size  = queue.getSize.unsafeRunSync()
      size shouldBe 0
    }

  }

}
