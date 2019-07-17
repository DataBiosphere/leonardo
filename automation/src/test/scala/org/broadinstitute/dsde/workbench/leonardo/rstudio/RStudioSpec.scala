package org.broadinstitute.dsde.workbench.leonardo.rstudio

import org.broadinstitute.dsde.workbench.fixture.BillingFixtures
import org.broadinstitute.dsde.workbench.leonardo._
import org.broadinstitute.dsde.workbench.service.util.Tags
import org.scalatest.{FreeSpec, ParallelTestExecution}
import scala.util.Try

class RStudioSpec extends FreeSpec with LeonardoTestUtils with ParallelTestExecution with BillingFixtures {

  "Leonardo clusters" - {

    "should install RStudio" taggedAs Tags.SmokeTest in {
      withProject { project => implicit token =>
        withNewCluster(project, request = defaultClusterRequest.copy(rstudioDockerImage = Some("us.gcr.io/broad-dsp-gcr-public/leonardo-rstudio:860d5862f3f5"))) { cluster =>
          withWebDriver {implicit driver =>
            val getResult = Try(RStudio.getApi(project, cluster.clusterName))
            getResult.isSuccess shouldBe true
            getResult.get should not include "ProxyException"
          }
        }
      }
    }
  }
}
