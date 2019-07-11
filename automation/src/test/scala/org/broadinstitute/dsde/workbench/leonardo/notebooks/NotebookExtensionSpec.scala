package org.broadinstitute.dsde.workbench.leonardo.notebooks

import akka.http.scaladsl.model.HttpResponse
import org.broadinstitute.dsde.workbench.ResourceFile
import org.broadinstitute.dsde.workbench.google2.GcsBlobName
import org.broadinstitute.dsde.workbench.leonardo._
import org.scalatest.time.{Minutes, Seconds, Span}

import scala.concurrent.duration._
import scala.language.postfixOps


class NotebookExtensionSpec extends ClusterFixtureSpec with NotebookTestUtils {
  override def enableWelder: Boolean = true

  debug = true
  mockedCluster = mockCluster("gpalloc-dev-master-3crdvwj","automation-test-aveenp4rz")
//
  "Leonardo welder and jupyter extensions" - {
//
//    "Welder should be up" in { clusterFixture =>
//      println("printing cluster Fixture")
//      println(clusterFixture)
//      val resp: HttpResponse = Welder.getWelderStatus(clusterFixture.cluster)
//      resp.status.isSuccess() shouldBe true
//    }

    "open notebook in edit mode should work" in { clusterFixture =>
        val sampleNotebook = ResourceFile("bucket-tests/gcsFile.ipynb")
      val isEditMode = true
        withResourceFileInBucket(clusterFixture.billingProject, sampleNotebook, "text/plain") { googleCloudDir =>
          logger.info("Initialized google storage bucket")

          withWelderInitialized(clusterFixture.cluster, googleCloudDir, isEditMode) { localizedFile =>
            withWebDriver { implicit driver =>

            withOpenNotebook(clusterFixture.cluster, localizedFile, 200.minutes) { notebookPage => //TODO fix timeout

              notebookPage.modeExists() shouldBe true
              notebookPage.getMode() shouldBe Notebook.EditMode
              notebookPage.addCodeAndExecute("1+1")
              notebookPage.clickSave()

              val localContentSize: Long = Notebook.getNotebookItem(clusterFixture.billingProject, clusterFixture.cluster.clusterName, Welder.getLocalPath(googleCloudDir, isEditMode)).size.toLong

              eventually(timeout(Span(5, Seconds))) {
                val remoteContentSize: Long = getObjectAsFile(googleCloudDir.bucketName, GcsBlobName(googleCloudDir.objectName.value)).length()
                remoteContentSize shouldBe localContentSize
              }

              //TODO: wait should be removed/changed when https://broadworkbench.atlassian.net/browse/IA-1177 is fixed
              logger.info("Waiting 4 minutes as lock takes time to be reflected in metadata")
              eventually(timeout(Span(4, Minutes)), interval(Span(30, Seconds))) {
                val gcsLockedBy: Option[String] = getLockedBy(googleCloudDir.bucketName, GcsBlobName(googleCloudDir.objectName.value)).unsafeRunSync()
                val welderLockedBy: String = Welder.getMetadata(clusterFixture.cluster, googleCloudDir, isEditMode).lastLockedBy

                gcsLockedBy should not be null
                welderLockedBy should not be None
                gcsLockedBy shouldBe Some(welderLockedBy)
              }

//              notebookPage.close()
            }
          }
        }
        }
      }

//    "open notebook in playground mode should work" in { clusterFixture =>
//      val sampleNotebook = ResourceFile("bucket-tests/gcsFile.ipynb")
//val isEditMode = false
//      withResourceFileInBucket(clusterFixture.billingProject, sampleNotebook, "text/plain") { googleCloudDir =>
//        logger.info("Initialized google storage bucket")
//
//        withWelderInitialized(clusterFixture.cluster, googleCloudDir, isEditMode) { localizedFile =>
//          withWebDriver { implicit driver =>
//
//            withOpenNotebook(clusterFixture.cluster, localizedFile, 2.minutes) { notebookPage =>
//              logger.info("notebook is open")
//              notebookPage.modeExists() shouldBe true
//              notebookPage.getMode() shouldBe Notebook.SafeMode
//              notebookPage.addCodeAndExecute("1+1")
//              notebookPage.clickSave()
//
//    logger.info("Waiting 30 seconds to ensure notebook is saved to remote.")
//    Thread.sleep(5000)
//               val localContentSize: Long = Notebook.getNotebookItem(clusterFixture.billingProject, clusterFixture.cluster.clusterName, Welder.getLocalPath(googleCloudDir, isEditMode)).size.toLong
//
//              val remoteContentSize: Long = getObjectAsFile(googleCloudDir.bucketName, GcsBlobName(googleCloudDir.objectName.value)).length()
//
//
//
//              //some selectors are omitted to simplfiy the test with the assumption that if the majority are hidden, they all are
//              val uiElementIds: List[String]  = List("#save-notbook", "#new_notebook", "#open_notebook", "#copy_notebook", "#save_notebook_as", "#save_checkpoint", "#restore_checkpoint", "#notification_notebook", "#file_menu > li.divider:eq(0)", "#file_menu > li.divider:eq(2)")
//      //TODO verify UI elements gone
//              val areElementsHidden: Boolean = notebookPage.areElementsHidden(uiElementIds)
//
//              localContentSize should be > remoteContentSize
//              areElementsHidden shouldBe true
//
//              //TODO: wait should be removed/changed when https://broadworkbench.atlassian.net/browse/IA-1177 is fixed
//              logger.info("Waiting 4 minutes as lock takes time to be reflected in metadata")
//              eventually(timeout(Span(4, Minutes)), interval(Span(30, Seconds))) {
//                val gcsLockedBy: Option[String] = getLockedBy(googleCloudDir.bucketName, GcsBlobName(googleCloudDir.objectName.value)).unsafeRunSync()
//                val welderLockedBy: String = Welder.getMetadata(clusterFixture.cluster, googleCloudDir, isEditMode).lastLockedBy
//
//                gcsLockedBy shouldBe null
//                welderLockedBy shouldNe None
//              }
//            }
//          }
//        }
//      }
//    }

  }
}
