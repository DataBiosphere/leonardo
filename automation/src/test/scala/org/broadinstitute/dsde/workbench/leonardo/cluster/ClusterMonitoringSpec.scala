package org.broadinstitute.dsde.workbench.leonardo.cluster

import org.broadinstitute.dsde.workbench.fixture.BillingFixtures
import org.broadinstitute.dsde.workbench.leonardo.LeonardoTestUtils
import org.scalatest.{FreeSpec, ParallelTestExecution}

class ClusterMonitoringSpec extends FreeSpec with LeonardoTestUtils with ParallelTestExecution with BillingFixtures {

  "Leonardo clusters" - {

    "should save cluster error if the userscript fails" in {
      withProject { project => implicit token =>
        logger.info(s"${project.value}: should save cluster error if the userscript fails")

        withNewErroredCluster(project) { cluster =>
          cluster.errors.exists(_.errorMessage contains "Userscript failed.") should be true
        }
      }
    }
  }
}
