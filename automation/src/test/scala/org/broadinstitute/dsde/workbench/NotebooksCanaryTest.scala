package org.broadinstitute.dsde.workbench

import java.time.Instant
import java.util.UUID
import java.net.URL

import org.broadinstitute.dsde.workbench.leonardo.ClusterStatus.ClusterStatus
import org.broadinstitute.dsde.workbench.leonardo.StringValueClass.LabelMap
import org.broadinstitute.dsde.workbench.auth.AuthToken
import org.broadinstitute.dsde.workbench.leonardo.notebooks.{Notebook, NotebookTestUtils}
import org.scalatest.{FreeSpec, Matchers, ParallelTestExecution}
import org.scalatest.concurrent.ScalaFutures
import org.broadinstitute.dsde.workbench.fixture.BillingFixtures
import org.broadinstitute.dsde.workbench.leonardo.Leonardo.ApiVersion.V2
import org.broadinstitute.dsde.workbench.leonardo._
import org.broadinstitute.dsde.workbench.model.WorkbenchEmail
import org.broadinstitute.dsde.workbench.model.google.{GcsObjectName, GcsPath, GoogleProject}
import org.openqa.selenium.WebDriver
import org.scalatest.selenium.WebBrowser
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
//      val canaryCluster = "notebooks-canary-test-cluster"

//      withProject { project =>
//        implicit token =>
//
//          withNewCluster(project, monitorDelete = true, apiVersion = V2) { cluster =>
//            // verify that the cluster is running
//            cluster.status shouldBe ClusterStatus.Running
//
//            withWebDriver { implicit driver =>
//              withLocalizeDelocalizeFiles(cluster, localizeFileName, localizeFileContents, delocalizeFileName, delocalizeFileContents, localizeDataFileName, localizeDataContents) { (localizeRequest, bucketName, notebookPage) =>
//                Notebook.localize(project, cluster.clusterName, localizeRequest, async = false)
//                // verify that the files are at their locations
//                verifyLocalizeDelocalize(cluster, localizeFileName, localizeFileContents, GcsPath(bucketName, GcsObjectName(delocalizeFileName)), delocalizeFileContents, localizeDataFileName, localizeDataContents)
//              }
//            }
//            clusterName = cluster.clusterName
//          }
////          val deletedCluster = Leonardo.cluster.get(project, clusterName)
////          deletedCluster.status shouldBe ClusterStatus.Deleted
//          monitorDelete(project, clusterName)
//        println("cluster has been deleted")
//      }

//      withProject { canaryProject =>
//        implicit ronAuthToken =>
                //val clusterName = ClusterName("notebooks-canary-test-cluster")
          val canaryCluster = Cluster(
            ClusterName("notebooks-canary-test-cluster"),
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

            //startCluster(canaryProject, canaryCluster.clusterName, monitor = true)
         // val deletedCluster = Leonardo.cluster.get(canaryProject, canaryCluster.clusterName)

                    withWebDriver { implicit driver =>
                      withLocalizeDelocalizeFiles(canaryCluster, localizeFileName, localizeFileContents, delocalizeFileName, delocalizeFileContents, localizeDataFileName, localizeDataContents) { (localizeRequest, bucketName, notebookPage) =>
                        Notebook.localize(canaryProject, canaryCluster.clusterName, localizeRequest, async = false)
                        // verify that the files are at their locations
                        verifyLocalizeDelocalize(canaryCluster, localizeFileName, localizeFileContents, GcsPath(bucketName, GcsObjectName(delocalizeFileName)), delocalizeFileContents, localizeDataFileName, localizeDataContents)
                      }
                    }
//                  monitorDelete(canaryProject, canaryCluster.clusterName)
      deleteCluster(canaryProject, canaryCluster.clusterName, monitor = true)
                println("cluster has been deleted")
//                  }
      }
    }
  }


