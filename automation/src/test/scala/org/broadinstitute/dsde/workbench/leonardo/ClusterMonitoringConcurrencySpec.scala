package org.broadinstitute.dsde.workbench.leonardo

import org.broadinstitute.dsde.workbench.fixture.BillingFixtures
import org.broadinstitute.dsde.workbench.model.google.GoogleProject
import org.broadinstitute.dsde.workbench.service.{Orchestration, RestException}
import org.scalatest.{FreeSpec, ParallelTestExecution}

/**
  * Created by rtitle on 4/19/18.
  */
class ClusterMonitoringConcurrencySpec extends FreeSpec with LeonardoTestUtils with ParallelTestExecution with BillingFixtures {

  "Concurrent cluster requests" - {

    "create (ok) -> create (conflict)" in withWebDriver { implicit driver =>
      withCleanBillingProject(hermioneCreds) { projectName =>
        Orchestration.billing.addUserToBillingProject(projectName, ronEmail, Orchestration.billing.BillingProjectRole.User)(hermioneAuthToken)
        val project = GoogleProject(projectName)
        implicit val token = ronAuthToken

        withNewCluster(project, monitorCreate = false, monitorDelete = true) { cluster =>
          val caught = the [RestException] thrownBy createNewCluster(project, cluster.clusterName, monitor = false)
          caught.message should include (""""statusCode":409""")
        }
      }
    }

    "create (ok) -> delete (ok)" in withWebDriver { implicit driver =>
      withCleanBillingProject(hermioneCreds) { projectName =>
        Orchestration.billing.addUserToBillingProject(projectName, ronEmail, Orchestration.billing.BillingProjectRole.User)(hermioneAuthToken)
        val project = GoogleProject(projectName)
        implicit val token = ronAuthToken

        withNewCluster(project, monitorCreate = false, monitorDelete = true)(_ => ())
      }
    }

    "create (ok) -> wait -> delete (ok) -> create (conflict)" in withWebDriver { implicit driver =>
      withCleanBillingProject(hermioneCreds) { projectName =>
        Orchestration.billing.addUserToBillingProject(projectName, ronEmail, Orchestration.billing.BillingProjectRole.User)(hermioneAuthToken)
        val project = GoogleProject(projectName)
        implicit val token = ronAuthToken

        val cluster = withNewCluster(project, monitorCreate = true, monitorDelete = false)(identity)

        val caught = the [RestException] thrownBy createNewCluster(project, cluster.clusterName)
        caught.message should include (""""statusCode":409""")
      }
    }

    "create (ok) -> wait -> delete (ok) -> delete (conflict)" in withWebDriver { implicit driver =>
      withCleanBillingProject(hermioneCreds) { projectName =>
        Orchestration.billing.addUserToBillingProject(projectName, ronEmail, Orchestration.billing.BillingProjectRole.User)(hermioneAuthToken)
        val project = GoogleProject(projectName)
        implicit val token = ronAuthToken

        val cluster = withNewCluster(project, monitorCreate = true, monitorDelete = false)(identity)

        val caught = the [RestException] thrownBy deleteCluster(project, cluster.clusterName, monitor = false)
        caught.message should include (""""statusCode":409""")
      }
    }

    "create (ok) -> stop (409)" in withWebDriver { implicit driver =>
      withCleanBillingProject(hermioneCreds) { projectName =>
        Orchestration.billing.addUserToBillingProject(projectName, ronEmail, Orchestration.billing.BillingProjectRole.User)(hermioneAuthToken)
        val project = GoogleProject(projectName)
        implicit val token = ronAuthToken

        withNewCluster(project, monitorCreate = false, monitorDelete = true) { cluster =>
          val caught = the[RestException] thrownBy stopCluster(project, cluster.clusterName, monitor = false)
          caught.message should include (""""statusCode":409""")
        }
      }
    }

    "create (ok) -> wait -> stop (ok) -> start (ok)" in withWebDriver { implicit driver =>
      withCleanBillingProject(hermioneCreds) { projectName =>
        Orchestration.billing.addUserToBillingProject(projectName, ronEmail, Orchestration.billing.BillingProjectRole.User)(hermioneAuthToken)
        val project = GoogleProject(projectName)
        implicit val token = ronAuthToken

        withNewCluster(project, monitorCreate = true, monitorDelete = false) { cluster =>
          stopCluster(project, cluster.clusterName, monitor = false)
          startAndMonitor(project, cluster.clusterName)
        }
      }
    }
  }

}
