package org.broadinstitute.dsde.workbench.leonardo.cluster

import org.broadinstitute.dsde.workbench.auth.AuthToken
import org.broadinstitute.dsde.workbench.fixture.BillingFixtures
import org.broadinstitute.dsde.workbench.leonardo.{ClusterStatus, Leonardo, LeonardoTestUtils}
import org.broadinstitute.dsde.workbench.model.google.GoogleProject
import org.broadinstitute.dsde.workbench.service.RestException
import org.scalatest.{DoNotDiscover, FreeSpec, ParallelTestExecution}

/**
  * This spec is for validating how Leonardo/Google handles cluster status transitions.
  *
  * Note these tests can take a long time so we don't test all edge cases, but these cases
  * should exercise the most commonly used paths through the system.
  */
@DoNotDiscover
class ClusterStatusTransitionsSpec(val billingProject: GoogleProject) extends FreeSpec with LeonardoTestUtils with ParallelTestExecution with BillingFixtures {

  implicit val ronToken: AuthToken = ronAuthToken

  // these tests just hit the Leo APIs; they don't interact with notebooks via selenium
  "ClusterStatusTransitionsSpec" - {

    "create, monitor, delete should transition correctly" in {
      val clusterName = randomClusterName
      val clusterRequest = defaultClusterRequest

      // create a cluster, but don't wait
      createNewCluster(billingProject, clusterName, clusterRequest, monitor = false)

      // cluster status should be Creating
      val creatingCluster = Leonardo.cluster.get(billingProject, clusterName)
      creatingCluster.status shouldBe ClusterStatus.Creating

      // can't create another cluster with the same name
      val caught = the[RestException] thrownBy createNewCluster(billingProject, clusterName, monitor = false)
      caught.message should include(""""statusCode":409""")

      // can't stop a Creating cluster
      val caught2 = the[RestException] thrownBy stopCluster(billingProject, clusterName, monitor = false)
      caught2.message should include(""""statusCode":409""")

      // wait for cluster to be running
      monitorCreate(billingProject, clusterName, clusterRequest, creatingCluster)
      Leonardo.cluster.get(billingProject, clusterName).status shouldBe ClusterStatus.Running

      // delete the cluster, but don't wait
      deleteCluster(billingProject, clusterName, monitor = false)

      // cluster status should be Deleting
      Leonardo.cluster.get(billingProject, clusterName).status shouldBe ClusterStatus.Deleting

      // Call delete again. This should succeed, and not change the status.
      deleteCluster(billingProject, clusterName, monitor = false)
      Leonardo.cluster.get(billingProject, clusterName).status shouldBe ClusterStatus.Deleting

      // Can't recreate while cluster is deleting
      val caught3 = the[RestException] thrownBy createNewCluster(billingProject, clusterName, clusterRequest, monitor = false)
      caught3.message should include(""""statusCode":409""")

      // Wait for the cluster to be deleted
      monitorDelete(billingProject, clusterName)

      // New cluster can now be recreated with the same name
      // We monitor creation to make sure it gets successfully created in Google.
      withNewCluster(billingProject, clusterName, clusterRequest, monitorCreate = true, monitorDelete = false)(noop)
    }

    "error'd clusters should transition correctly" in {
      // make an Error'd cluster
      withNewErroredCluster(billingProject) { cluster =>
        // cluster should be in Error status
        cluster.status shouldBe ClusterStatus.Error

        // can't stop an Error'd cluster
        val caught = the[RestException] thrownBy stopCluster(cluster.googleProject, cluster.clusterName, monitor = false)
        caught.message should include(""""statusCode":409""")

        // can't recreate an Error'd cluster
        val caught2 = the[RestException] thrownBy createNewCluster(cluster.googleProject, cluster.clusterName, monitor = false)
        caught2.message should include(""""statusCode":409""")

        // can delete an Error'd cluster
      }
    }

    // set the "stop after creation" flag
    "should stop a cluster after creation" in {
      val request = defaultClusterRequest.copy(stopAfterCreation = Some(true))
      withNewCluster(billingProject, request = request) { cluster =>
        cluster.stopAfterCreation shouldBe true
        Leonardo.cluster.get(billingProject, cluster.clusterName).status shouldBe ClusterStatus.Stopped
      }
    }

    // Note: omitting stop/start and patch/update tests here because those are covered in more depth in NotebookClusterMonitoringSpec
  }

}
