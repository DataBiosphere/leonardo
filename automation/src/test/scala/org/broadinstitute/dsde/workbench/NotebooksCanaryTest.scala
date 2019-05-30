package org.broadinstitute.dsde.workbench

import java.time.Instant
import java.util.UUID
import java.net.URL
import java.sql.Time
import java.util.Timer

import akka.actor.FSM
import akka.actor.FSM.Timer
import org.broadinstitute.dsde.workbench.auth.AuthToken
import org.broadinstitute.dsde.workbench.leonardo.notebooks.{Notebook, NotebookTestUtils, Python2, Python3}
import org.scalatest.Matchers
import org.broadinstitute.dsde.workbench.fixture.BillingFixtures
import org.broadinstitute.dsde.workbench.leonardo.Leonardo.ApiVersion.V2
import org.broadinstitute.dsde.workbench.model.google.{GcsObjectName, GcsPath, GoogleProject}
import org.scalatest.{FreeSpec, ParallelTestExecution}
import org.broadinstitute.dsde.workbench.leonardo.{cluster, _}
import org.broadinstitute.dsde.workbench.model.WorkbenchEmail
import scala.language.postfixOps
import sys.process._


class NotebooksCanaryTest extends FreeSpec with Matchers with NotebookTestUtils with ParallelTestExecution with
  BillingFixtures {

  implicit val authToken: AuthToken = ronAuthToken
  "Test for creating a cluster and localizing a notebook" - {

    "should launch a notebook" in {

      val localizeFileName = "localize_sync.txt"
      val localizeFileContents = "Sync localize test"
      val delocalizeFileName = "delocalize_sync.txt"
      val delocalizeFileContents = "Sync delocalize test"
      val localizeDataFileName = "localize_data_aync.txt"
      val localizeDataContents = "Hello World"

      val project = GoogleProject("automated-notebooks-canary")

      val clusterTimeRes = time(withNewCluster(project, monitorDelete = true, apiVersion = V2) { cluster =>
        val clusterStatus = cluster.status
        withWebDriver { implicit driver =>
          withNewNotebook(cluster, Python3) { notebook =>
            print(notebook.executeCell("""print("hi")""") shouldBe (Some("hi")))
            clusterStatus shouldBe(ClusterStatus.Running)
          }
        }
        withWebDriver { implicit driver =>
          withLocalizeDelocalizeFiles(cluster, localizeFileName, localizeFileContents, delocalizeFileName, delocalizeFileContents, localizeDataFileName, localizeDataContents) { (localizeRequest, bucketName, notebookPage) =>
            Notebook.localize(project, cluster.clusterName, localizeRequest, async = false)
            verifyLocalizeDelocalize(cluster, localizeFileName, localizeFileContents, GcsPath(bucketName, GcsObjectName(delocalizeFileName)), delocalizeFileContents, localizeDataFileName, localizeDataContents)
          }
        }
        println("end of test")
      })
      logger.info(s"Time it took to complete test: " +
        s" ${clusterTimeRes.duration.toSeconds}")
    }
  }

  val res = true

    if (res) {
      (s"./notebooks-canary-test-script.sh $clusterTimeRes" !!)
    }
  }

