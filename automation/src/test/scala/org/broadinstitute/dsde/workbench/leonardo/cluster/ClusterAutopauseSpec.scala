package org.broadinstitute.dsde.workbench.leonardo.cluster

import org.broadinstitute.dsde.workbench.auth.AuthToken
import org.broadinstitute.dsde.workbench.leonardo.{ClusterStatus, GPAllocFixtureSpec, Leonardo, LeonardoTestUtils}
import org.scalatest.time.{Seconds, Span}
import org.scalatest.{DoNotDiscover, ParallelTestExecution}

@DoNotDiscover
class ClusterAutopauseSpec extends GPAllocFixtureSpec with ParallelTestExecution with LeonardoTestUtils {

  implicit val ronToken: AuthToken = ronAuthToken

  "autopause should work" in { billingProject =>

    val clusterName = randomClusterName
    val clusterRequest = defaultClusterRequest.copy(autopause = Some(true), autopauseThreshold = Some(1))

    withNewCluster(billingProject, clusterName, clusterRequest) { cluster =>
      Leonardo.cluster
        .get(cluster.googleProject, cluster.clusterName)
        .autopauseThreshold shouldBe 1

      eventually(timeout(Span(90, Seconds)), interval(Span(10, Seconds))) {
        val dbCluster = Leonardo.cluster.get(cluster.googleProject, cluster.clusterName)
        dbCluster.autopauseThreshold shouldBe 1
        dbCluster.status shouldBe ClusterStatus.Stopping
      }
    }

  }
}
