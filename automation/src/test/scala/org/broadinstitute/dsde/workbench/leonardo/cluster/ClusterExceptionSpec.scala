package org.broadinstitute.dsde.workbench.leonardo.cluster

import org.broadinstitute.dsde.workbench.fixture.BillingFixtures
import org.broadinstitute.dsde.workbench.leonardo.Leonardo.ApiVersion.V2
import org.broadinstitute.dsde.workbench.leonardo.LeonardoTestUtils
import org.broadinstitute.dsde.workbench.service.RestException
import org.scalatest.{FreeSpec, ParallelTestExecution}

class ClusterExceptionSpec extends FreeSpec with LeonardoTestUtils with ParallelTestExecution with BillingFixtures {

  "Leonardo clusters" - {

    // create -> wait -> error -> stop (conflict) -> delete
    "should not be able to stop an errored cluster" in {
      withProject { project => implicit token =>
        logger.info(s"${project.value}: should not be able to stop an errored cluster")

        withNewErroredCluster(project) { cluster =>
          withWebDriver { implicit driver =>
            val caught = the[RestException] thrownBy stopCluster(cluster.googleProject, cluster.clusterName, monitor = false)
            caught.message should include(""""statusCode":409""")
          }
        }
      }
    }

    // create -> create again (conflict) -> delete
    "should not be able to create 2 clusters with the same name" in {
      withProject { project => implicit token =>
        logger.info(s"${project.value}: should not be able to create 2 clusters with the same name")

        withNewCluster(project, monitorCreate = false, monitorDelete = true, apiVersion = V2) { cluster =>
          val caught = the[RestException] thrownBy createNewCluster(project, cluster.clusterName, monitor = false)
          caught.message should include(""""statusCode":409""")
        }
      }
    }

    // create -> wait -> delete -> no wait -> create (conflict)
    "should not be able to recreate a deleting cluster" in {
      withProject { project => implicit token =>
        logger.info(s"${project.value}: should not be able to recreate a deleting cluster")

        val cluster = withNewCluster(project, monitorCreate = true)(identity)

        val caught = the[RestException] thrownBy createNewCluster(project, cluster.clusterName)
        caught.message should include(""""statusCode":409""")

        monitorDelete(project, cluster.clusterName)
      }
    }


    // create -> no wait -> stop (conflict) -> delete
    "should not be able to stop a creating cluster" in {
      withProject { project => implicit token =>
        logger.info(s"${project.value}: should not be able to stop a creating cluster")

        withNewCluster(project, monitorCreate = false, monitorDelete = true, apiVersion = V2) { cluster =>
          withWebDriver { implicit driver =>
            val caught = the[RestException] thrownBy stopCluster(project, cluster.clusterName, monitor = false)
            caught.message should include(""""statusCode":409""")
          }
        }
      }
    }

  }

}
