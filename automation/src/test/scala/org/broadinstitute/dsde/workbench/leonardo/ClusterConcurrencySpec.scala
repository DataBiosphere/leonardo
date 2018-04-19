package org.broadinstitute.dsde.workbench.leonardo

import org.broadinstitute.dsde.workbench.fixture.BillingFixtures
import org.broadinstitute.dsde.workbench.service.RestException
import org.scalatest.{FreeSpec, ParallelTestExecution}

/**
  * Created by rtitle on 4/26/18.
  */
class ClusterConcurrencySpec extends FreeSpec with LeonardoTestUtils with ParallelTestExecution with BillingFixtures {

  // these tests just hit the Leo APIs; they don't interact with notebooks via selenium
  "Leonardo cluster status transitions" - {

    // (create -> wait -> delete -> wait) * 2
    "should create, monitor, delete, recreate, and re-delete a cluster" in {
      withProject { project => implicit token =>
<<<<<<< HEAD
        logger.info(s"${project.value}: should create, monitor, delete, recreate, and re-delete a cluster")

        val nameToReuse = randomClusterName

        // create, monitor, delete once
        withNewCluster(project, nameToReuse)(noop)

        // create, monitor, delete again with same name
        withNewCluster(project, nameToReuse)(noop)
=======
        val nameToReuse = randomClusterName

        // create, monitor, delete once
        withNewCluster(project, nameToReuse)(identity)

        // create, monitor, delete again with same name
        withNewCluster(project, nameToReuse)(identity)
>>>>>>> 253f5efa... Better status checking
      }
    }

    // create -> wait -> error -> delete
    "should delete an errored out cluster" in {
      withProject { project => implicit token =>
<<<<<<< HEAD
        logger.info(s"${project.value}: should delete an errored out cluster")

=======
>>>>>>> 253f5efa... Better status checking
        withNewErroredCluster(project) { _ =>
          // no-op; just verify that it launches
        }
      }
    }

    // create -> no wait -> delete
    "should delete a creating cluster" in withWebDriver { implicit driver =>
      withProject { project => implicit token =>
<<<<<<< HEAD
        logger.info(s"${project.value}: should delete a creating cluster")

        // delete while the cluster is still creating
        withNewCluster(project, monitorCreate = false, monitorDelete = true)(noop)
=======
        // delete while the cluster is still creating
        withNewCluster(project, monitorCreate = false, monitorDelete = true)(identity)
>>>>>>> 253f5efa... Better status checking
      }
    }

    // create -> create again (conflict) -> delete
    "should not be able to create 2 clusters with the same name" in withWebDriver { implicit driver =>
      withProject { project => implicit token =>
<<<<<<< HEAD
        logger.info(s"${project.value}: should not be able to create 2 clusters with the same name")

=======
>>>>>>> 253f5efa... Better status checking
        withNewCluster(project, monitorCreate = false, monitorDelete = true) { cluster =>
          val caught = the[RestException] thrownBy createNewCluster(project, cluster.clusterName, monitor = false)
          caught.message should include(""""statusCode":409""")
        }
      }
    }

    // create -> wait -> delete -> no wait -> create (conflict)
    "should not be able to recreate a deleting cluster" in withWebDriver { implicit driver =>
      withProject { project => implicit token =>
<<<<<<< HEAD
        logger.info(s"${project.value}: should not be able to recreate a deleting cluster")

=======
>>>>>>> 253f5efa... Better status checking
        val cluster = withNewCluster(project, monitorCreate = true, monitorDelete = false)(identity)

        val caught = the[RestException] thrownBy createNewCluster(project, cluster.clusterName)
        caught.message should include(""""statusCode":409""")

        monitorDelete(project, cluster.clusterName)
      }
    }

    // create -> wait -> delete -> no wait -> delete
    "should not be able to delete a deleting cluster" in withWebDriver { implicit driver =>
      withProject { project => implicit token =>
<<<<<<< HEAD
        logger.info(s"${project.value}: should not be able to delete a deleting cluster")

=======
>>>>>>> 253f5efa... Better status checking
        val cluster = withNewCluster(project, monitorCreate = true, monitorDelete = false)(identity)

        // second delete should succeed
        deleteAndMonitor(project, cluster.clusterName)
      }
    }

    // create -> no wait -> stop (conflict) -> delete
    "should not be able to stop a creating cluster" in withWebDriver { implicit driver =>
      withProject { project => implicit token =>
<<<<<<< HEAD
        logger.info(s"${project.value}: should not be able to stop a creating cluster")

=======
>>>>>>> 253f5efa... Better status checking
        withNewCluster(project, monitorCreate = false, monitorDelete = true) { cluster =>
          val caught = the[RestException] thrownBy stopCluster(project, cluster.clusterName, monitor = false)
          caught.message should include(""""statusCode":409""")
        }
      }
    }

    // create -> wait -> stop -> no wait -> start -> delete
    "should be able to start a stopping cluster" in withWebDriver { implicit driver =>
      withProject { project => implicit token =>
<<<<<<< HEAD
        logger.info(s"${project.value}: should be able to start a stopping cluster")

=======
>>>>>>> 253f5efa... Better status checking
        withNewCluster(project) { cluster =>
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
    "should be able to delete a stopped cluster" in withWebDriver { implicit driver =>
      withProject { project => implicit token =>
<<<<<<< HEAD
        logger.info(s"${project.value}: should be able to delete a stopped cluster")

=======
>>>>>>> 253f5efa... Better status checking
        withNewCluster(project) { cluster =>
          // delete after stop is complete
          stopAndMonitor(cluster.googleProject, cluster.clusterName)
        }
      }
    }

    // create -> wait -> stop -> no wait -> delete
    "should be able to delete a stopping cluster" in withWebDriver { implicit driver =>
      withProject { project => implicit token =>
<<<<<<< HEAD
        logger.info(s"${project.value}: should be able to delete a stopping cluster")

=======
>>>>>>> 253f5efa... Better status checking
        withNewCluster(project) { cluster =>
          // delete without waiting for the stop to complete
          stopCluster(cluster.googleProject, cluster.clusterName, monitor = false)
        }
      }
    }
  }
}
