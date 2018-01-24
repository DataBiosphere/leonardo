package org.broadinstitute.dsde.workbench.leonardo

import org.broadinstitute.dsde.workbench.service.Orchestration
import org.broadinstitute.dsde.workbench.dao.Google.googleIamDAO
import org.scalatest.{FreeSpec, ParallelTestExecution}
import org.scalatest.time.{Seconds, Span}

class ClusterMonitoringSpec extends FreeSpec with LeonardoTestUtils with ParallelTestExecution {
  "Leonardo clusters" - {

    "should create, monitor, and delete a cluster" in {
      withNewBillingProject { project =>
        Orchestration.billing.addUserToBillingProject(project.value, ronEmail, Orchestration.billing.BillingProjectRole.User)(hermioneAuthToken)
        implicit val token = ronAuthToken
        withNewCluster(project) { _ =>
          // no-op; just verify that it launches
        }
      }
    }

    "should error on cluster create and delete the cluster" in {
      withNewBillingProject { project =>
        Orchestration.billing.addUserToBillingProject(project.value, ronEmail, Orchestration.billing.BillingProjectRole.User)(hermioneAuthToken)
        implicit val token = ronAuthToken
        withNewErroredCluster(project) { _ =>
          // no-op; just verify that it launches
        }
      }
    }

    "should create a cluster in a different billing project and put the pet's credentials on the cluster" in withWebDriver { implicit driver =>
      withNewBillingProject { project =>
        Orchestration.billing.addUserToBillingProject(project.value, ronEmail, Orchestration.billing.BillingProjectRole.User)(hermioneAuthToken)

        implicit val token = ronAuthToken
        // Pre-conditions: pet service account exists in this Google project and in Sam
        val (petName, petEmail) = getAndVerifyPet(project)

        // Create a cluster

        withNewCluster(project) { cluster =>
          // cluster should have been created with the pet service account
          cluster.serviceAccountInfo.clusterServiceAccount shouldBe Some(petEmail)
          cluster.serviceAccountInfo.notebookServiceAccount shouldBe None

          withNewNotebook(cluster) { notebookPage =>
            // should not have notebook credentials because Leo is not configured to use a notebook service account
            verifyNoNotebookCredentials(notebookPage)
          }
        }

        // Post-conditions: pet should still exist in this Google project

        val googlePetEmail2 = googleIamDAO.findServiceAccount(project, petName).futureValue.map(_.email)
        googlePetEmail2 shouldBe Some(petEmail)
      }
    }

  }

}
