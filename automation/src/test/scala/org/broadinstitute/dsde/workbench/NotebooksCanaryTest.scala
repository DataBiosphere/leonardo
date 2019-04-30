package org.broadinstitute.dsde.workbench

import java.util.concurrent.TimeUnit

import org.broadinstitute.dsde.workbench.auth.AuthToken
import org.broadinstitute.dsde.workbench.config.{Credentials, LeoAuthToken, UserPool}
import org.broadinstitute.dsde.workbench.fixture.BillingFixtures
import org.broadinstitute.dsde.workbench.leonardo.notebooks.{Notebook, NotebookTestUtils}
import org.scalatest.{FreeSpec, Matchers, ParallelTestExecution}
import org.scalatest.concurrent.ScalaFutures
import org.broadinstitute.dsde.workbench.dao.Google.{googleIamDAO, googleStorageDAO}
import org.broadinstitute.dsde.workbench.fixture.BillingFixtures
import org.broadinstitute.dsde.workbench.leonardo.Leonardo.ApiVersion.V2
import org.broadinstitute.dsde.workbench.leonardo._
import org.broadinstitute.dsde.workbench.model.google.{GcsObjectName, GcsPath}
import org.broadinstitute.dsde.workbench.service.util.Tags
import org.openqa.selenium.WebDriver
import org.openqa.selenium.chrome.ChromeDriver
import org.scalatest.selenium.WebBrowser
import org.scalatest.time.{Millis, Seconds, Span}
import org.scalatest.{FreeSpec, ParallelTestExecution}
import org.scalatest.concurrent.TimeLimitedTests

import scala.concurrent.duration._

class NotebooksCanaryTest extends FreeSpec with Matchers with NotebookTestUtils with ParallelTestExecution with
  BillingFixtures {

  //  val timeLimit = Span(1, Seconds)

  // commented out with TimeLimitedTests

  implicit val authToken: AuthToken = hermioneAuthToken
  "Test for creating a cluster and localizing a notebook" - {

    "should launch a notebook" in {


      val localizeFileName = "localize_sync.txt"
      val localizeFileContents = "Sync localize test"
      val delocalizeFileName = "delocalize_sync.txt"
      val delocalizeFileContents = "Sync delocalize test"
      val localizeDataFileName = "localize_data_aync.txt"
      val localizeDataContents = "Hello World"

      var clusterName = ClusterName("")

      withProject { project =>
        implicit token =>

          withNewCluster(project, monitorDelete = true, apiVersion = V2) { cluster =>
            // verify that the cluster is running
            cluster.status shouldBe ClusterStatus.Running

            Thread.sleep(2000)

            withWebDriver { implicit driver =>
              withLocalizeDelocalizeFiles(cluster, localizeFileName, localizeFileContents, delocalizeFileName, delocalizeFileContents, localizeDataFileName, localizeDataContents) { (localizeRequest, bucketName, notebookPage) =>
                Notebook.localize(project, cluster.clusterName, localizeRequest, async = false)
                // verify that the files are at their locations
                verifyLocalizeDelocalize(cluster, localizeFileName, localizeFileContents, GcsPath(bucketName, GcsObjectName(delocalizeFileName)), delocalizeFileContents, localizeDataFileName, localizeDataContents)
              }
            }
            clusterName = cluster.clusterName
          }
//          val deletedCluster = Leonardo.cluster.get(project, clusterName)
//          deletedCluster.status shouldBe ClusterStatus.Deleted
          monitorDelete(project, clusterName)
        println("cluster has been deleted")

      }
    }
  }
}

