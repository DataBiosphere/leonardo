package org.broadinstitute.dsde.workbench.leonardo.runtimes

import org.broadinstitute.dsde.workbench.auth.AuthToken
import org.broadinstitute.dsde.workbench.leonardo.{ClusterStatus, GPAllocFixtureSpec, Leonardo, LeonardoTestUtils}
import org.scalatest.time.{Minutes, Seconds, Span}
import org.scalatest.{DoNotDiscover, ParallelTestExecution}

@DoNotDiscover
class RuntimeAutopauseSpec extends GPAllocFixtureSpec with ParallelTestExecution with LeonardoTestUtils {

  implicit val ronToken: AuthToken = ronAuthToken

  "autopause should work" in { billingProject =>
    val runtimeName = randomClusterName
    val runtimeRequest = defaultRuntimeRequest.copy(autopause = Some(true), autopauseThreshold = Some(1))

    withNewRuntime(billingProject, runtimeName, runtimeRequest) { runtime =>
      Leonardo.cluster
        .getRuntime(runtime.googleProject, runtime.clusterName)
        .autopauseThreshold shouldBe 1

      //the autopause check interval is 1 minute at the time of creation, but it can be flaky with a tighter window.
      eventually(timeout(Span(3, Minutes)), interval(Span(10, Seconds))) {
        val dbCluster = Leonardo.cluster.getRuntime(runtime.googleProject, runtime.clusterName)
        dbCluster.status shouldBe ClusterStatus.Stopping
      }
    }

  }
}
