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

  "Should perform end to end pub/sub flow" taggedAs Tags.SmokeTest in { clusterFixture =>
    val publisher = GooglePublisher.resource[IO, String](LeonardoConfig.Leonardo.publisherConfig)
    publisher.use {
      _ => IO.unit
    }
    for {
      googlePublisher <- GooglePublisher.resource[F, LeoPubsubMessage](LeonardoConfig.Leonardo.publisherConfig)

      publisherQueue <- Resource.liftF(InspectableQueue.bounded[F, LeoPubsubMessage](100))
      publisherStream = publisherQueue.dequeue through googlePublisher.publish

      subscriberQueue <- Resource.liftF(InspectableQueue.bounded[F, Event[LeoPubsubMessage]](100))
      subscriber <- GoogleSubscriber.resource(subscriberConfig, subscriberQueue)

      pubsubReader = new LeoPubsubMessageSubscriber()
    } yield ()
    //      pubsubReader = new MessageReader(subscriber, clusterHelper, dbRef)
    //      subscriberStream = pubsubReader.process
  }



}
