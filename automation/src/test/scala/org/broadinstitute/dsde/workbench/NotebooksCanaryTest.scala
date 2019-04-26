package org.broadinstitute.dsde.workbench

import org.broadinstitute.dsde.workbench.auth.AuthToken
import org.broadinstitute.dsde.workbench.config.{Credentials, LeoAuthToken, UserPool}
import org.broadinstitute.dsde.workbench.fixture.BillingFixtures
import org.broadinstitute.dsde.workbench.leonardo.notebooks.{Notebook, NotebookTestUtils}
import org.scalatest.{FreeSpec, Matchers, ParallelTestExecution}
import org.scalatest.concurrent.ScalaFutures
import org.broadinstitute.dsde.workbench.ResourceFile
import org.broadinstitute.dsde.workbench.dao.Google.{googleIamDAO, googleStorageDAO}
import org.broadinstitute.dsde.workbench.fixture.BillingFixtures
import org.broadinstitute.dsde.workbench.leonardo.Leonardo.ApiVersion.V2
import org.broadinstitute.dsde.workbench.leonardo._
import org.broadinstitute.dsde.workbench.model.google.GcsEntityTypes.Group
import org.broadinstitute.dsde.workbench.model.google.GcsRoles.Reader
import org.broadinstitute.dsde.workbench.model.google.{EmailGcsEntity, GcsObjectName, GcsPath, parseGcsPath}
import org.broadinstitute.dsde.workbench.service.util.Tags
import org.scalatest.time.{Seconds, Span}
import org.scalatest.{FreeSpec, ParallelTestExecution}
import scala.concurrent.duration._

class NotebooksCanaryTest extends FreeSpec with Matchers with NotebookTestUtils with ParallelTestExecution with BillingFixtures {

  implicit val authToken: AuthToken = hermioneAuthToken
  "Test for creating a cluster and localizing a notebook" - {

    "should launch a notebook" in {
      //      val user = LeoAuthToken.getUserToken("dumbledore.admin@test.firecloud.org")

      val localizeFileName = "localize_sync.txt"
      val localizeFileContents = "Sync localize test"
      val delocalizeFileName = "delocalize_sync.txt"
      val delocalizeFileContents = "Sync delocalize test"
      val localizeDataFileName = "localize_data_aync.txt"
      val localizeDataContents = "Hello World"

      withProject { project =>
        implicit token =>
          withNewCluster(project, monitorDelete = true, apiVersion = V2) { cluster =>
            Leonardo.cluster.start(project, cluster.clusterName)
            withLocalizeDelocalizeFiles(cluster, localizeFileName, localizeFileContents, delocalizeFileName, delocalizeFileContents, localizeDataFileName, localizeDataContents) { (localizeRequest, bucketName, notebookPage) =>
              Notebook.localize(project, cluster.clusterName, localizeRequest, async = false)
              //Notebook.setCookie(project, cluster.clusterName)
              //cleanup step
              Leonardo.cluster.delete(project, cluster.clusterName)
            }
          }
      }
    }
  }
}

