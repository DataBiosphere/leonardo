package org.broadinstitute.dsde.workbench.leonardo.runtimes

import org.broadinstitute.dsde.workbench.auth.AuthToken
import org.broadinstitute.dsde.workbench.leonardo._
import org.broadinstitute.dsde.workbench.service.RestException
import org.scalatest.tagobjects.Retryable
import org.scalatest.{DoNotDiscover, ParallelTestExecution}

/**
 * This spec is for validating how Leonardo/Google handles cluster status transitions.
 *
 * Note these tests can take a long time so we don't test all edge cases, but these cases
 * should exercise the most commonly used paths through the system.
 */
//@DoNotDiscover
class RuntimeStatusTransitionsSpec extends GPAllocFixtureSpec with ParallelTestExecution with LeonardoTestUtils with GPAllocBeforeAndAfterAll {

  // these tests just hit the Leo APIs; they don't interact with notebooks via selenium
  "RuntimeStatusTransitionsSpec" - {

    implicit val ronToken: AuthToken = ronAuthToken

    "create, monitor, delete should transition correctly" in { billingProject =>
      logger.info("Starting ClusterStatusTransitionsSpec: create, monitor, delete should transition correctly")

      val clusterName = randomClusterName
      val clusterRequest = defaultRuntimeRequest

      // create a cluster, but don't wait
      createNewRuntime(billingProject, clusterName, clusterRequest, monitor = false)

      // cluster status should be Creating
      val creatingCluster = Leonardo.cluster.getRuntime(billingProject, clusterName)
      creatingCluster.status shouldBe ClusterStatus.Creating

      // can't create another cluster with the same name
      val caught = the[RestException] thrownBy createNewRuntime(billingProject, clusterName, monitor = false)
      caught.message should include(""""statusCode":409""")

      // can't stop a Creating cluster
      val caught2 = the[RestException] thrownBy stopRuntime(billingProject, clusterName, monitor = false)
      caught2.message should include(""""statusCode":409""")

      // wait for cluster to be running
      monitorCreateRuntime(billingProject, clusterName, clusterRequest, creatingCluster)
      Leonardo.cluster.getRuntime(billingProject, clusterName).status shouldBe ClusterStatus.Running

      // delete the cluster, but don't wait
      deleteRuntime(billingProject, clusterName, monitor = false)

      // cluster status should be Deleting
      Leonardo.cluster.getRuntime(billingProject, clusterName).status shouldBe ClusterStatus.Deleting

      // Call delete again. This should succeed, and not change the status.
      deleteRuntime(billingProject, clusterName, monitor = false)
      Leonardo.cluster.getRuntime(billingProject, clusterName).status shouldBe ClusterStatus.Deleting

      // Can't recreate while cluster is deleting
      val caught3 = the[RestException] thrownBy createNewRuntime(billingProject,
                                                                 clusterName,
                                                                 clusterRequest,
                                                                 monitor = false)
      caught3.message should include(""""statusCode":409""")

      // Wait for the cluster to be deleted
      monitorDeleteRuntime(billingProject, clusterName)

      // New cluster can now be recreated with the same name
      // We monitor creation to make sure it gets successfully created in Google.
      withNewRuntime(billingProject, clusterName, clusterRequest, monitorCreate = true, monitorDelete = false)(noop)
    }

//    "error'd clusters should transition correctly" in { billingProject =>
//      logger.info("Starting ClusterStatusTransitionsSpec: error'd clusters should transition correctly")
//
//      // make an Error'd cluster
//      withNewErroredCluster(billingProject) { cluster =>
//        // cluster should be in Error status
//        cluster.status shouldBe ClusterStatus.Error
//
//        // can't stop an Error'd cluster
//        val caught = the[RestException] thrownBy stopCluster(cluster.googleProject,
//                                                             cluster.clusterName,
//                                                             monitor = false)
//        caught.message should include(""""statusCode":409""")
//
//        // can't recreate an Error'd cluster
//        val caught2 = the[RestException] thrownBy createNewCluster(cluster.googleProject,
//                                                                   cluster.clusterName,
//                                                                   monitor = false)
//        caught2.message should include(""""statusCode":409""")
//
//        // can delete an Error'd cluster
//      }
//    }
//
//    // set the "stop after creation" flag
//    "should stop a cluster after creation" taggedAs (Retryable) in { billingProject =>
//      logger.info("Starting ClusterStatusTransitionsSpec: should stop a cluster after creation")
//
//      val request = defaultClusterRequest.copy(stopAfterCreation = Some(true))
//      withNewCluster(billingProject, request = request) { cluster =>
//        cluster.stopAfterCreation shouldBe true
//        Leonardo.cluster.get(billingProject, cluster.clusterName).status shouldBe ClusterStatus.Stopped
//      }
//    }

    // Note: omitting stop/start and patch/update tests here because those are covered in more depth in NotebookClusterMonitoringSpec
  }

}
