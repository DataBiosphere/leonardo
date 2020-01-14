package org.broadinstitute.dsde.workbench.leonardo

import cats.effect.IO
import org.broadinstitute.dsde.workbench.google2.GooglePublisher
import org.broadinstitute.dsde.workbench.leonardo.notebooks.Welder
import org.broadinstitute.dsde.workbench.service.util.Tags
import org.scalatest.{BeforeAndAfterAll, DoNotDiscover, fixture}

@DoNotDiscover
class LeoPubsubSpec extends ClusterFixtureSpec with BeforeAndAfterAll with LeonardoTestUtils {

  "Google publisher should be able to auth" taggedAs Tags.SmokeTest in { clusterFixture =>
    val publisher = GooglePublisher.resource[IO, String](LeonardoConfig.Leonardo.publisherConfig)
    publisher.use {
      _ => IO.unit
    }
  }
}
