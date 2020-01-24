package org.broadinstitute.dsde.workbench.leonardo

import cats.effect.IO
import fs2.concurrent.InspectableQueue
import org.broadinstitute.dsde.workbench.google2.GooglePublisher
import org.broadinstitute.dsde.workbench.service.util.Tags
import org.scalatest.time.{Minutes, Seconds, Span}
import org.scalatest.DoNotDiscover

@DoNotDiscover
class LeoPubsubSpec extends ClusterFixtureSpec with LeonardoTestUtils {

  "Google publisher should be able to auth" taggedAs Tags.SmokeTest in { _ =>
    logger.info(s"publisher config is: ${LeonardoConfig.Leonardo.publisherConfig}")

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

    "Google subscriber should receive a published message" in { clusterFixture =>
      val newMasterMachineType = Some("n1-standard-2")
      val machineConfig = Some(MachineConfig(masterMachineType = newMasterMachineType))

      val originalCluster = Leonardo.cluster.get(clusterFixture.cluster.googleProject, clusterFixture.cluster.clusterName)
      originalCluster.status shouldBe ClusterStatus.Running

      val originalMachineConfig = originalCluster.machineConfig

      Leonardo.cluster.update(clusterFixture.cluster.googleProject, clusterFixture.cluster.clusterName,
        clusterRequest = defaultClusterRequest.copy(allowStop = Some(true), machineConfig = machineConfig))

      eventually(timeout(Span(1, Minutes)), interval(Span(10, Seconds))) {
        val getCluster: Cluster = Leonardo.cluster.get(clusterFixture.cluster.googleProject, clusterFixture.cluster.clusterName)
        getCluster.status shouldBe ClusterStatus.Stopping
      }

      eventually(timeout(Span(10, Minutes)), interval(Span(30, Seconds))) {
        val getCluster: Cluster = Leonardo.cluster.get(clusterFixture.cluster.googleProject, clusterFixture.cluster.clusterName)
        getCluster.status shouldBe ClusterStatus.Running
        getCluster.machineConfig shouldBe originalMachineConfig.copy(masterMachineType = newMasterMachineType)
      }
    }

}
