package org.broadinstitute.dsde.workbench.leonardo

import cats.effect.IO
import fs2.concurrent.InspectableQueue
import org.broadinstitute.dsde.workbench.google2.{GooglePublisher, GoogleTopicAdmin}
import org.broadinstitute.dsde.workbench.service.util.Tags
import org.scalatest.time.{Minutes, Span}
import org.scalatest.{BeforeAndAfterAll, DoNotDiscover}

@DoNotDiscover
class LeoPubsubSpec extends ClusterFixtureSpec with BeforeAndAfterAll with LeonardoTestUtils {

  "Google publisher should be able to auth" taggedAs Tags.SmokeTest in { clusterFixture =>
    val publisher = GooglePublisher.resource[IO, String](LeonardoConfig.Leonardo.publisherConfig)
    publisher.use {
      _ => IO.unit
    }
      .unsafeRunSync()
  }

  "Google publisher should publish" in { _ =>
    val publisher = GooglePublisher.resource[IO, String](LeonardoConfig.Leonardo.publisherConfig)
    val queue = InspectableQueue.bounded[IO, String](100).unsafeRunSync()

    publisher.use { publisher =>
      (queue.dequeue through publisher.publish)
        .compile
        .drain
    }
      .unsafeRunAsync(_ => ())

    queue.enqueue1("automation-test-message").unsafeRunSync()

    eventually(timeout(Span(2, Minutes))) {
      val size = queue.getSize.unsafeRunSync()
      size shouldBe 0
    }
  }

    "Google subscriber should receive a published message" ignore { clusterFixture =>
      val machineConfig = Some(MachineConfig(masterMachineType = Some("n1-standard-2")))

      Leonardo.cluster.update(clusterFixture.cluster.googleProject, clusterFixture.cluster.clusterName,
        clusterRequest = defaultClusterRequest.copy(allowStop = Some(true), machineConfig = machineConfig))

      eventually(timeout(Span(1, Minutes))) {
        val getCluster: Cluster = Leonardo.cluster.get(clusterFixture.cluster.googleProject, clusterFixture.cluster.clusterName)
        getCluster.status shouldBe ClusterStatus.Stopping
      }

      eventually(timeout(Span(3, Minutes))) {
        val getCluster: Cluster = Leonardo.cluster.get(clusterFixture.cluster.googleProject, clusterFixture.cluster.clusterName)
        getCluster.status shouldBe ClusterStatus.Starting
        getCluster.machineConfig shouldBe machineConfig
      }
    }

  override def afterAll(): Unit = {
    cleanupTopic().unsafeRunSync()
    super.afterAll()
  }

  def cleanupTopic(): IO[Unit] = {
    GoogleTopicAdmin.fromCredentialPath(LeonardoConfig.Leonardo.publisherConfig.pathToCredentialJson)
      .use { client =>
        //TODO: uncomment once workbench libs PR is in
//        client.delete(LeonardoConfig.Leonardo.publisherConfig.projectTopicName)
//          .compile
//          .drain
        IO.unit
      }
  }

}
