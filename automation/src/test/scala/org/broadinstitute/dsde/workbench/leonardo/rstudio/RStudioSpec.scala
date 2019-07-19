package org.broadinstitute.dsde.workbench.leonardo.rstudio

import org.broadinstitute.dsde.workbench.auth.AuthToken
import org.broadinstitute.dsde.workbench.fixture.BillingFixtures
import org.broadinstitute.dsde.workbench.leonardo._
import org.broadinstitute.dsde.workbench.model.google.GoogleProject
import org.broadinstitute.dsde.workbench.service.util.Tags
import org.scalatest.{DoNotDiscover, FreeSpec, ParallelTestExecution}

import scala.util.Try

@DoNotDiscover
class RStudioSpec(val billingProject: GoogleProject) extends FreeSpec with LeonardoTestUtils with BillingFixtures {

  implicit val ronToken: AuthToken = ronAuthToken

  "Leonardo clusters" - {

    // TODO re-enable when RStudio is supported
    "should install RStudio" taggedAs Tags.SmokeTest ignore {
      withNewCluster(billingProject, request = defaultClusterRequest.copy(rstudioDockerImage = Some("us.gcr.io/broad-dsp-gcr-public/leonardo-rstudio:860d5862f3f5"))) { cluster =>
        withWebDriver {implicit driver =>
          val getResult = Try(RStudio.getApi(billingProject, cluster.clusterName))
          getResult.isSuccess shouldBe true
          getResult.get should not include "ProxyException"
        }
      }
    }
  }
}
