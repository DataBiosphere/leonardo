package org.broadinstitute.dsde.workbench.leonardo

import org.broadinstitute.dsde.workbench.auth.AuthToken
import org.broadinstitute.dsde.workbench.fixture.BillingFixtures
import org.broadinstitute.dsde.workbench.model.google.GoogleProject
import org.broadinstitute.dsde.workbench.service.RestException
import org.scalatest.{BeforeAndAfterAll, FreeSpec, ParallelTestExecution}

/**
  * Created by rtitle on 4/26/18.
  */
class ClusterConcurrencySpec extends FreeSpec with BeforeAndAfterAll with LeonardoTestUtils with ParallelTestExecution with BillingFixtures {

  var gpAllocProject: ClaimedProject = _
  var billingProject: GoogleProject = _
  implicit val ronToken: AuthToken = ronAuthToken

  override def beforeAll(): Unit = {
    super.beforeAll()
    gpAllocProject = claimGPAllocProject(hermioneCreds, List(ronEmail))
    billingProject = GoogleProject(gpAllocProject.projectName)
  }

  override def afterAll(): Unit = {
    gpAllocProject.cleanup(hermioneCreds, List(ronEmail))
    super.afterAll()
  }

  // these tests just hit the Leo APIs; they don't interact with notebooks via selenium
  "Leonardo cluster status transitions" - {

    // (create -> wait -> delete -> wait) * 2
    "should create, monitor, delete, recreate, and re-delete a cluster" in {
      logger.info(s"${billingProject.value}: should create, monitor, delete, recreate, and re-delete a cluster")

      val nameToReuse = randomClusterName

      // create, monitor, delete once
      withNewCluster(billingProject, nameToReuse)(noop)

      // create, monitor, delete again with same name
      withNewCluster(billingProject, nameToReuse)(noop)
    }

    // create -> wait -> error -> delete
    "should delete an errored out cluster" in {
      logger.info(s"${billingProject.value}: should delete an errored out cluster")

      withNewErroredCluster(billingProject) { _ =>
        // no-op; just verify that it launches
      }
    }

    // create -> no wait -> delete
    "should delete a creating cluster" in withWebDriver { implicit driver =>
      logger.info(s"${billingProject.value}: should delete a creating cluster")

      // delete while the cluster is still creating
      withNewCluster(billingProject, monitorCreate = false, monitorDelete = true)(noop)
    }

    // create -> create again (conflict) -> delete
    "should not be able to create 2 clusters with the same name" in withWebDriver { implicit driver =>
      logger.info(s"${billingProject.value}: should not be able to create 2 clusters with the same name")

      withNewCluster(billingProject, monitorCreate = false, monitorDelete = true) { cluster =>
        val caught = the[RestException] thrownBy createNewCluster(billingProject, cluster.clusterName, monitor = false)
        caught.message should include(""""statusCode":409""")
      }
    }

    // create -> wait -> delete -> no wait -> create (conflict)
    "should not be able to recreate a deleting cluster" in withWebDriver { implicit driver =>
      logger.info(s"${billingProject.value}: should not be able to recreate a deleting cluster")

      val cluster = withNewCluster(billingProject, monitorCreate = true, monitorDelete = false)(identity)

      val caught = the[RestException] thrownBy createNewCluster(billingProject, cluster.clusterName)
      caught.message should include(""""statusCode":409""")

      monitorDelete(billingProject, cluster.clusterName)
    }

    // create -> wait -> delete -> no wait -> delete
    "should not be able to delete a deleting cluster" in withWebDriver { implicit driver =>
      logger.info(s"${billingProject.value}: should not be able to delete a deleting cluster")

      val cluster = withNewCluster(billingProject, monitorCreate = true, monitorDelete = false)(identity)

      // second delete should succeed
      deleteAndMonitor(billingProject, cluster.clusterName)
    }

    // create -> no wait -> stop (conflict) -> delete
    "should not be able to stop a creating cluster" in withWebDriver { implicit driver =>
      logger.info(s"${billingProject.value}: should not be able to stop a creating cluster")

      withNewCluster(billingProject, monitorCreate = false, monitorDelete = true) { cluster =>
        val caught = the[RestException] thrownBy stopCluster(billingProject, cluster.clusterName, monitor = false)
        caught.message should include(""""statusCode":409""")
      }
    }

    // create -> wait -> stop -> no wait -> start -> delete
    "should be able to start a stopping cluster" in withWebDriver { implicit driver =>
      logger.info(s"${billingProject.value}: should be able to start a stopping cluster")

      withNewCluster(billingProject) { cluster =>
        // start without waiting for stop to complete
        stopCluster(billingProject, cluster.clusterName, monitor = false)
        // TODO: cluster is not _immediately_ startable after stopping. Why?
        logger.info("Sleeping 10 seconds before starting")
        Thread.sleep(10000)
        startAndMonitor(billingProject, cluster.clusterName)
      }
    }

    // create -> wait -> stop -> wait -> delete
    "should be able to delete a stopped cluster" in withWebDriver { implicit driver =>
      logger.info(s"${billingProject.value}: should be able to delete a stopped cluster")

      withNewCluster(billingProject) { cluster =>
        // delete after stop is complete
        stopAndMonitor(cluster.googleProject, cluster.clusterName)
      }
    }
  }

  // create -> wait -> stop -> no wait -> delete
  "should be able to delete a stopping cluster" in withWebDriver { implicit driver =>
    logger.info(s"${billingProject.value}: should be able to delete a stopping cluster")

    withNewCluster(billingProject) { cluster =>
      // delete without waiting for the stop to complete
      stopCluster(cluster.googleProject, cluster.clusterName, monitor = false)
    }
  }
}
