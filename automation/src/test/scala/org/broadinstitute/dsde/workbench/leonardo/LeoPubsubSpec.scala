package org.broadinstitute.dsde.workbench.leonardo

import cats.effect.{IO, Resource}
import fs2.concurrent.InspectableQueue
import org.broadinstitute.dsde.workbench.google2.{Event, GooglePublisher, GoogleSubscriber}
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
