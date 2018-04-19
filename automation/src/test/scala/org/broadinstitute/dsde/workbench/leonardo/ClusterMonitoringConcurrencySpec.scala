package org.broadinstitute.dsde.workbench.leonardo

import org.broadinstitute.dsde.workbench.fixture.BillingFixtures
import org.broadinstitute.dsde.workbench.model.google.GoogleProject
import org.broadinstitute.dsde.workbench.service.Orchestration
import org.scalatest.{FreeSpec, ParallelTestExecution}

/**
  * Created by rtitle on 4/19/18.
  */
class ClusterMonitoringConcurrencySpec extends FreeSpec with LeonardoTestUtils with ParallelTestExecution with BillingFixtures {

  "Concurrent cluster requests" - {

    "create (ok) -> create (conflict)" in {
      withCleanBillingProject(hermioneCreds) { projectName =>
        Orchestration.billing.addUserToBillingProject(projectName, ronEmail, Orchestration.billing.BillingProjectRole.User)(hermioneAuthToken)
        val project = GoogleProject(projectName)
        implicit val token = ronAuthToken


      }
    }
  }

}
