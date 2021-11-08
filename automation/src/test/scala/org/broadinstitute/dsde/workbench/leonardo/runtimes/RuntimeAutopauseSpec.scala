package org.broadinstitute.dsde.workbench.leonardo.runtimes

import org.broadinstitute.dsde.workbench.auth.AuthToken
import org.broadinstitute.dsde.workbench.leonardo.TestUser.Ron
import org.broadinstitute.dsde.workbench.leonardo.{
  ClusterStatus,
  GPAllocFixtureSpec,
  Leonardo,
  LeonardoApiClient,
  LeonardoTestUtils
}
import org.http4s.AuthScheme
import org.http4s.headers.Authorization
import org.scalatest.time.{Minutes, Seconds, Span}
import org.scalatest.{DoNotDiscover, ParallelTestExecution}

import scala.concurrent.duration._

@DoNotDiscover
class RuntimeAutopauseSpec extends GPAllocFixtureSpec with ParallelTestExecution with LeonardoTestUtils {
  implicit def ronToken: AuthToken = Ron.authToken()
  implicit val auth: Authorization = Authorization(org.http4s.Credentials.Token(AuthScheme.Bearer, ronToken.value))

  "autopause should work" in { billingProject =>
    val runtimeName = randomClusterName
    val runtimeRequest =
      LeonardoApiClient.defaultCreateRuntime2Request.copy(autopause = Some(true), autopauseThreshold = Some(1 minute))

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
