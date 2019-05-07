package org.broadinstitute.dsde.workbench

import java.time.Instant
import java.util.UUID
import java.net.URL

import org.broadinstitute.dsde.workbench.auth.AuthToken
import org.broadinstitute.dsde.workbench.leonardo.notebooks.{Notebook, NotebookTestUtils}
import org.scalatest.Matchers
import org.broadinstitute.dsde.workbench.fixture.BillingFixtures
import org.broadinstitute.dsde.workbench.leonardo._
import org.broadinstitute.dsde.workbench.model.WorkbenchEmail
import org.broadinstitute.dsde.workbench.model.google.{GcsObjectName, GcsPath, GoogleProject}
import org.scalatest.{FreeSpec, ParallelTestExecution}

class NotebooksCanaryTest extends FreeSpec with Matchers with NotebookTestUtils with ParallelTestExecution with
  BillingFixtures {

  object StringValueClass {
    type LabelMap = Map[String, String]
  }

  implicit val authToken: AuthToken = ronAuthToken
  "Test for creating a cluster and localizing a notebook" - {

    "should launch a notebook" in {

      val localizeFileName = "localize_sync.txt"
      val localizeFileContents = "Sync localize test"
      val delocalizeFileName = "delocalize_sync.txt"
      val delocalizeFileContents = "Sync delocalize test"
      val localizeDataFileName = "localize_data_aync.txt"
      val localizeDataContents = "Hello World"

      val canaryProject = GoogleProject("automated-notebooks-canary")

          val canaryCluster = Cluster(
            ClusterName("cluster-notebook"),
            UUID fromString("6aca69be-cf15-40d1-ac76-6c4207f52da8"),
            canaryProject,
            ServiceAccountInfo.apply(None, None),
            MachineConfig(None),
            new URL("https://leonardo.dsde-alpha.broadinstitute.org/notebooks/automated-notebooks-canary/notebooks-canary-test-cluster"),
            OperationName(""),
            ClusterStatus(0),
            None,
            WorkbenchEmail(""),
            Instant.ofEpochSecond(0),
            None,
            Map(),
            None,
            None,
            None,
            List(),
            Instant.ofEpochSecond(0),
            None,
            false,
            Set()
            )

      // create new cluster
      // localize
      // open a notebook withNewNotebook


      // withNewCluster
      // with new notebook
      //notebook.execute("hi")
      // verify execution
      // withLocalizeDe
      // run once an hour

                    withWebDriver { implicit driver =>
                      withLocalizeDelocalizeFiles(canaryCluster, localizeFileName, localizeFileContents, delocalizeFileName, delocalizeFileContents, localizeDataFileName, localizeDataContents) { (localizeRequest, bucketName, notebookPage) =>
                        Notebook.localize(canaryProject, canaryCluster.clusterName, localizeRequest, async = false)
                        verifyLocalizeDelocalize(canaryCluster, localizeFileName, localizeFileContents, GcsPath(bucketName, GcsObjectName(delocalizeFileName)), delocalizeFileContents, localizeDataFileName, localizeDataContents)
                      }
                    }
                println("cluster has been deleted")
      }
    }
  }
