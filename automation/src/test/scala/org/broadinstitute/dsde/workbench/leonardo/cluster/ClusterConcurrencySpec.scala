package org.broadinstitute.dsde.workbench.leonardo.cluster

import org.broadinstitute.dsde.workbench.fixture.BillingFixtures
import org.broadinstitute.dsde.workbench.leonardo.Leonardo.ApiVersion.V2
import org.broadinstitute.dsde.workbench.leonardo.{ClusterStatus, Leonardo, LeonardoTestUtils}
import org.broadinstitute.dsde.workbench.service.util.Tags
import org.scalatest.time.{Seconds, Span}
import org.scalatest.{FreeSpec, ParallelTestExecution}


/**
  * Created by rtitle on 4/26/18.
  */
class ClusterConcurrencySpec extends FreeSpec with LeonardoTestUtils with ParallelTestExecution with BillingFixtures {

  // these tests just hit the Leo APIs; they don't interact with notebooks via selenium
  "Leonardo cluster status transitions" - {

    // (create -> wait -> delete -> wait) * 2
    "should create, monitor, delete, recreate, and re-delete a cluster" taggedAs Tags.SmokeTest in {
      withProject { project => implicit token =>
        logger.info(s"${project.value}: should create, monitor, delete, recreate, and re-delete a cluster")

        val nameToReuse = randomClusterName

        // create, monitor, delete once
        withNewCluster(project, nameToReuse, monitorDelete = true)(noop)

        // create, monitor, delete again with same name
        withNewCluster(project, nameToReuse, apiVersion = V2, monitorDelete = true)(noop)
      }
    }

    // create -> wait -> error -> delete
    "should delete an errored out cluster" in {
      withProject { project => implicit token =>
        logger.info(s"${project.value}: should delete an errored out cluster")

        withNewErroredCluster(project) { _ =>
          // no-op; just verify that it launches
        }
      }
    }

    // create -> wait -> delete -> no wait -> delete
    "deleting an already deleting cluster should be a no-op" in {
      withProject { project => implicit token =>
        logger.info(s"${project.value}: deleting an already deleting cluster should be a no-op")

        // create a cluster. Delete cluster but don't wait
        val cluster = withNewCluster(project, monitorCreate = true)(identity)

        // cluster status is Deleting before test can proceed further
        withClue(s"Waiting for Cluster ${cluster.clusterName} status to become Deleting. ") {
          eventually(timeout(Span(300, Seconds)), interval(Span(30, Seconds))) {
            val status = Leonardo.cluster.get(project, cluster.clusterName).status
            status shouldBe ClusterStatus.Deleting
          }
        }

        // second delete call should be a no-op
        deleteCluster(project, cluster.clusterName, monitor=false)

        // sleep 2 seconds. Then check cluster status is unchanged
        Thread.sleep(2000)
        withClue(s"Cluster ${cluster.clusterName} status Deleting should be unchanged") {
          eventually(timeout(Span(2, Seconds))) {
            val status = Leonardo.cluster.get(project, cluster.clusterName).status
            status shouldBe ClusterStatus.Deleting
          }
        }
      }
    }

    // TODO: ignoring this test because it's not completely deterministic.
    // Sometimes Google will fail to start a Stopping cluster.
    // https://github.com/DataBiosphere/leonardo/issues/648 is open to
    // revisit this behavior in Leo.
    //
    // create -> wait -> stop -> no wait -> start -> delete
    "should be able to start a stopping cluster" ignore {
      withProject { project => implicit token =>
        logger.info(s"${project.value}: should be able to start a stopping cluster")

        withNewCluster(project, apiVersion = V2) { cluster =>
          // start without waiting for stop to complete
          stopCluster(project, cluster.clusterName, monitor = false)
          // TODO: cluster is not _immediately_ startable after stopping. Why?
          logger.info("Sleeping 10 seconds before starting")
          Thread.sleep(10000)
          startAndMonitor(project, cluster.clusterName)
        }
      }
    }

    // create -> wait -> stop -> wait -> delete
    "should be able to delete a stopped cluster" in {
      withProject { project => implicit token =>
        logger.info(s"${project.value}: should be able to delete a stopped cluster")

        withNewCluster(project) { cluster =>
          // delete after stop is complete
          stopAndMonitor(cluster.googleProject, cluster.clusterName)(token)
        }
      }
    }

    // create -> wait -> stop -> no wait -> delete
    "should be able to delete a stopping cluster" in {
      withProject { project => implicit token =>
        logger.info(s"${project.value}: should be able to delete a stopping cluster")
        withWebDriver { _ =>
          withNewCluster(project, apiVersion = V2) { cluster =>
            // delete without waiting for the stop to complete
            stopCluster(cluster.googleProject, cluster.clusterName, monitor = false)
          }
        }
      }
    }

    // set the "stop after creation" flag
    "should stop a cluster after creation" in {
      withProject { project => implicit token =>
        val request = defaultClusterRequest.copy(stopAfterCreation = Some(true))
        withNewCluster(project, request = request) { cluster =>
          cluster.stopAfterCreation shouldBe true
        }
      }
    }

    // make sure defaultClientId works
    "should set a default client id if specified" in {
      withProject { project => implicit token =>
        val request = defaultClusterRequest.copy(defaultClientId = Some("this is a client ID"))
        withNewCluster(project, request = request) { cluster =>
          cluster.defaultClientId shouldBe Some("this is a client ID")
          cluster.stopAfterCreation shouldBe false
        }
      }

    }
  }

}
