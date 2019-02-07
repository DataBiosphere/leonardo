package org.broadinstitute.dsde.workbench.leonardo.lab

import org.broadinstitute.dsde.workbench.leonardo.ClusterFixtureSpec
import org.broadinstitute.dsde.workbench.service.util.Tags
import org.scalatest.FreeSpec
import org.broadinstitute.dsde.workbench.leonardo._


import scala.util.Try



class LabSpec extends ClusterFixtureSpec with FreeSpec with LabTestUtils {

  "Leonardo lab" - {

    "should install JupyterLab and localize/delocalize" taggedAs Tags.SmokeTest in {
      withProject { project => implicit token =>
        withNewCluster(project, request = ClusterRequest()) { cluster =>
          withWebDriver { implicit driver =>
            // Check that the /lab URL is accessible
            val getResult = Try(Lab.getApi(project, cluster.clusterName))
            getResult.isSuccess shouldBe true
            getResult.get should not include "ProxyException"

            // Check that localization still works
            // See https://github.com/DataBiosphere/leonardo/issues/417, where installing JupyterLab
            // broke the initialization of jupyter_localize_extension.py.
            val localizeFileName = "localize_sync.txt"
            val localizeFileContents = "Sync localize test"
            val delocalizeFileName = "delocalize_sync.txt"
            val delocalizeFileContents = "Sync delocalize test"
            val localizeDataFileName = "localize_data_aync.txt"
            val localizeDataContents = "Hello World"

            withLocalizeDelocalizeFiles(cluster, localizeFileName, localizeFileContents, delocalizeFileName, delocalizeFileContents, localizeDataFileName, localizeDataContents) { (localizeRequest, bucketName, notebookPage) =>
              // call localize; this should return 200
              Notebook.localize(cluster.googleProject, cluster.clusterName, localizeRequest, async = false)

              // check that the files are immediately at their destinations
              verifyLocalizeDelocalize(cluster, localizeFileName, localizeFileContents, GcsPath(bucketName, GcsObjectName(delocalizeFileName)), delocalizeFileContents, localizeDataFileName, localizeDataContents)
            }
          }
        }
      }
    }

    "should execute cells" taggedAs Tags.SmokeTest in { clusterFixture =>
      withWebDriver { implicit driver =>
        withNewLabNotebook(clusterFixture.cluster) { labNotebookPage =>
          labNotebookPage.runCodeInEmptyCell("1+1") shouldBe Some("2")
          labNotebookPage.runCodeInEmptyCell("2*3") shouldBe Some("6")
          labNotebookPage.runCodeInEmptyCell("""print 'Hello Notebook!'""") shouldBe Some("Hello Notebook!")
        }
      }
    }
  }
}
