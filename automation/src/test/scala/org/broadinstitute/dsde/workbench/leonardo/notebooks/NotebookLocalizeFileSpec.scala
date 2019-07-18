package org.broadinstitute.dsde.workbench.leonardo.notebooks

import org.broadinstitute.dsde.workbench.dao.Google.googleStorageDAO
import org.broadinstitute.dsde.workbench.leonardo.ClusterFixtureSpec
import org.broadinstitute.dsde.workbench.model.google.{GcsObjectName, GcsPath}
import org.broadinstitute.dsde.workbench.service.RestException
import org.broadinstitute.dsde.workbench.service.util.Tags

import scala.language.postfixOps

/**
  * This spec verifies legacy notebook localization/delocalization, pre-welder.
  * Remove this once welder is fully rolled out.
  */
class NotebookLocalizeFileSpec extends ClusterFixtureSpec with NotebookTestUtils {

  override def enableWelder: Boolean = false

  "NotebookLocalizeFileSpec" - {

    "should localize files in async mode" in { clusterFixture =>
      val localizeFileName = "localize_async.txt"
      val localizeFileContents = "Async localize test"
      val delocalizeFileName = "delocalize_async.txt"
      val delocalizeFileContents = "Async delocalize test"
      val localizeDataFileName = "localize_data_async.txt"
      val localizeDataContents = "Hello World"

      withWebDriver { implicit driver =>
        val cluster = clusterFixture.cluster
        withLocalizeDelocalizeFiles(cluster, localizeFileName, localizeFileContents, delocalizeFileName, delocalizeFileContents, localizeDataFileName, localizeDataContents) { (localizeRequest, bucketName, notebookPage) =>
          // call localize; this should return 200
          Notebook.localize(cluster.googleProject, cluster.clusterName, localizeRequest, async = true)

          // check that the files are eventually at their destinations
          implicit val patienceConfig: PatienceConfig = localizePatience
          eventually {
            verifyLocalizeDelocalize(cluster, localizeFileName, localizeFileContents, GcsPath(bucketName, GcsObjectName(delocalizeFileName)), delocalizeFileContents, localizeDataFileName, localizeDataContents)
          }

          // call localize again with bad data. This should still return 200 since we're in async mode.
          val badLocalize = Map("file.out" -> "gs://nobuckethere")
          Notebook.localize(cluster.googleProject, cluster.clusterName, badLocalize, async = true)

          // it should not have localized this file
          val thrown = the[RestException] thrownBy {
            Notebook.getContentItem(cluster.googleProject, cluster.clusterName, "file.out", includeContent = false)
          }
          // why doesn't `RestException` have a status code field?
          thrown.message should include("No such file or directory: file.out")
        }
      }
    }

    "should localize files in sync mode" taggedAs Tags.SmokeTest in { clusterFixture =>
      val localizeFileName = "localize_sync.txt"
      val localizeFileContents = "Sync localize test"
      val delocalizeFileName = "delocalize_sync.txt"
      val delocalizeFileContents = "Sync delocalize test"
      val localizeDataFileName = "localize_data_aync.txt"
      val localizeDataContents = "Hello World"

      withWebDriver { implicit driver =>
        val cluster = clusterFixture.cluster
        withLocalizeDelocalizeFiles(cluster, localizeFileName, localizeFileContents, delocalizeFileName, delocalizeFileContents, localizeDataFileName, localizeDataContents) { (localizeRequest, bucketName, notebookPage) =>
          // call localize; this should return 200
          Notebook.localize(cluster.googleProject, cluster.clusterName, localizeRequest, async = false)

          // check that the files are immediately at their destinations
          verifyLocalizeDelocalize(cluster, localizeFileName, localizeFileContents, GcsPath(bucketName, GcsObjectName(delocalizeFileName)), delocalizeFileContents, localizeDataFileName, localizeDataContents)

          // call localize again with bad data. This should still return 500 since we're in sync mode.
          val badLocalize = Map("file.out" -> "gs://nobuckethere")
          val thrown = the[RestException] thrownBy {
            Notebook.localize(cluster.googleProject, cluster.clusterName, badLocalize, async = false)
          }
          // why doesn't `RestException` have a status code field?
          thrown.message should include("500 : Internal Server Error")
          thrown.message should include("Error occurred localizing")
          thrown.message should include("See localization.log for details.")

          // it should not have localized this file
          val contentThrown = the[RestException] thrownBy {
            Notebook.getContentItem(cluster.googleProject, cluster.clusterName, "file.out", includeContent = false)
          }
          contentThrown.message should include("No such file or directory: file.out")
        }
      }
    }

    "should localize files with spaces in the name" in { clusterFixture =>
      val localizeFileName = "localize with spaces.txt"
      val localizeFileContents = "Localize"
      val delocalizeFileName = "delocalize with spaces.txt"
      val delocalizeFileContents = "Delocalize"
      val localizeDataFileName = "localize data with spaces.txt"
      val localizeDataContents = "Localize data"

      withWebDriver { implicit driver =>
        val cluster = clusterFixture.cluster
        withLocalizeDelocalizeFiles(cluster, localizeFileName, localizeFileContents, delocalizeFileName, delocalizeFileContents, localizeDataFileName, localizeDataContents) { (localizeRequest, bucketName, notebookPage) =>
          // call localize; this should return 200
          Notebook.localize(cluster.googleProject, cluster.clusterName, localizeRequest, async = false)

          // check that the files are at their destinations
          implicit val patienceConfig: PatienceConfig = storagePatience

          // the localized files should exist on the notebook VM
          notebookPage.executeCell(s"""! cat "$localizeFileName"""") shouldBe Some(localizeFileContents)
          notebookPage.executeCell(s"""! cat "$localizeDataFileName"""") shouldBe Some(localizeDataContents)

          // the delocalized file should exist in the Google bucket
          val bucketData = googleStorageDAO.getObject(bucketName, GcsObjectName(delocalizeFileName)).futureValue
          bucketData.map(_.toString) shouldBe Some(delocalizeFileContents)
        }
      }
    }

  }

}
