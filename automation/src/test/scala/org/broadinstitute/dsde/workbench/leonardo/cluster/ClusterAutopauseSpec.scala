package org.broadinstitute.dsde.workbench.leonardo.cluster

import org.broadinstitute.dsde.workbench.leonardo.{Cluster, ClusterFixtureSpec, ClusterStatus, Leonardo, LeonardoTestUtils}
import org.scalatest.time.{Minutes, Seconds, Span}
import org.scalatest.DoNotDiscover

//@DoNotDiscover
class ClusterAutopauseSpec extends ClusterFixtureSpec with LeonardoTestUtils {

  "patch autopause then pause properly" in { clusterFixture =>
    val x: Cluster = Leonardo.cluster.get(clusterFixture.cluster.googleProject, clusterFixture.cluster.clusterName)

    logger.info(s"original autopause threshold: ${x.autopauseThreshold}")

    Leonardo.cluster.update(
      clusterFixture.cluster.googleProject,
      clusterFixture.cluster.clusterName,
      clusterRequest = defaultClusterRequest.copy(autopause = Some(true), autopauseThreshold = Some(1))
    )

    eventually(timeout(Span(2, Minutes)), interval(Span(30, Seconds))) {
      val cluster = Leonardo.cluster.get(clusterFixture.cluster.googleProject, clusterFixture.cluster.clusterName)
      cluster.autopauseThreshold shouldBe 1
      cluster.status shouldBe ClusterStatus.Stopping
    }

  }
}
