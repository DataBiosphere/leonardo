package org.broadinstitute.dsde.workbench.leonardo.cluster

import org.broadinstitute.dsde.workbench.leonardo.{ClusterFixtureSpec, ClusterStatus, Leonardo, LeonardoTestUtils}
import org.scalatest.time.{Minutes, Seconds, Span}
import org.scalatest.DoNotDiscover

@DoNotDiscover
class ClusterAutopauseSpec extends ClusterFixtureSpec with LeonardoTestUtils {

  "patch autopause then pause properly" in { clusterFixture =>
    Leonardo.cluster
      .get(clusterFixture.cluster.googleProject, clusterFixture.cluster.clusterName)
      .autopauseThreshold shouldBe 0

    Leonardo.cluster.update(
      clusterFixture.cluster.googleProject,
      clusterFixture.cluster.clusterName,
      clusterRequest = defaultClusterRequest.copy(autopause = Some(true), autopauseThreshold = Some(1))
    )

    eventually(timeout(Span(5, Minutes)), interval(Span(10, Seconds))) {
      val cluster = Leonardo.cluster.get(clusterFixture.cluster.googleProject, clusterFixture.cluster.clusterName)
      cluster.autopauseThreshold shouldBe 1
      cluster.status shouldBe ClusterStatus.Stopping
    }

  }
}
