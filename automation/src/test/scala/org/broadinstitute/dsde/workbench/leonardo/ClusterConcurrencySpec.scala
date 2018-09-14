package org.broadinstitute.dsde.workbench.leonardo

import org.broadinstitute.dsde.workbench.fixture.BillingFixtures
import org.broadinstitute.dsde.workbench.leonardo.Leonardo.ApiVersion.V2
import org.broadinstitute.dsde.workbench.service.util.Tags
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
        withNewCluster(project, nameToReuse)(noop)

        // create, monitor, delete again with same name
        withNewCluster(project, nameToReuse, apiVersion = V2)(noop)
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

    // create -> no wait -> delete
    "should delete a creating cluster" in {
      withProject { project => implicit token =>
        logger.info(s"${project.value}: should delete a creating cluster")

        // delete while the cluster is still creating
        withNewCluster(project, monitorCreate = false, monitorDelete = true, apiVersion = V2)(noop)
      }
    }

    // create -> wait -> delete -> no wait -> delete
    "should not be able to delete a deleting cluster" in {
      withProject { project => implicit token =>
        logger.info(s"${project.value}: should not be able to delete a deleting cluster")

        val cluster = withNewCluster(project, monitorCreate = true)(identity)

        // second delete should succeed
        deleteAndMonitor(project, cluster.clusterName)
      }
    }

    // create -> wait -> stop -> no wait -> start -> delete
    "should be able to start a stopping cluster" in {
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
        withWebDriver { implicit driver =>
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
          // no-op; just verify the cluster is stopped
        }
      }
    }

    // make sure defaultClientId works
    "should set a default client id if specified" in {
      withProject { project => implicit token =>
        val request = defaultClusterRequest.copy(defaultClientId = Some("this is a client ID"))
        withNewCluster(project, request = request) { cluster =>
          cluster.defaultClientId shouldBe Some("this is a client ID")
        }
      }

    }
  }

}
