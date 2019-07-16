package org.broadinstitute.dsde.workbench.leonardo.notebooks

import akka.http.scaladsl.model.HttpResponse
import org.broadinstitute.dsde.workbench.ResourceFile
import org.broadinstitute.dsde.workbench.google2.GcsBlobName
import org.broadinstitute.dsde.workbench.leonardo._
import org.broadinstitute.dsde.workbench.leonardo.notebooks.Notebook.NotebookMode
import org.scalatest.time.{Minutes, Seconds, Span}

import scala.concurrent.duration._
import scala.language.postfixOps


class NotebookExtensionSpec extends ClusterFixtureSpec with NotebookTestUtils {
  override def enableWelder: Boolean = true

  "Leonardo welder and jupyter extensions" - {

    "Welder should be up" in { clusterFixture =>
      val resp: HttpResponse = Welder.getWelderStatus(clusterFixture.cluster)
      resp.status.isSuccess() shouldBe true
    }

    "open notebook in edit mode should work" in { clusterFixture =>
      val sampleNotebook = ResourceFile("bucket-tests/gcsFile.ipynb")
      val isEditMode = true

      withResourceFileInBucket(clusterFixture.billingProject, sampleNotebook, "text/plain") { gcsPath =>

        withWelderInitialized(clusterFixture.cluster, gcsPath, isEditMode) { localizedFile =>

          withWebDriver { implicit driver =>

            withOpenNotebook(clusterFixture.cluster, localizedFile, 2.minutes) { notebookPage =>

              notebookPage.modeExists() shouldBe true
              notebookPage.getMode() shouldBe NotebookMode.EditMode
              notebookPage.addCodeAndExecute("1+1")
              notebookPage.saveNotebook()

              val localContentSize: Int = Notebook.getNotebookItem(clusterFixture.billingProject, clusterFixture.cluster.clusterName, Welder.getLocalPath(gcsPath, isEditMode)).size

              eventually(timeout(Span(5, Seconds))) {
                val remoteContentSize: Int = getObjectSize(gcsPath.bucketName, GcsBlobName(gcsPath.objectName.value))
                  .unsafeRunSync()

                remoteContentSize shouldBe localContentSize
              }

              logger.info("Waiting 4 minutes as lock takes time to be reflected in metadata")
              eventually(timeout(Span(4, Minutes)), interval(Span(30, Seconds))) {
                val gcsLockedBy: Option[String] = getLockedBy(gcsPath.bucketName, GcsBlobName(gcsPath.objectName.value)).unsafeRunSync()
                val welderLockedBy: Option[String] = Welder.getMetadata(clusterFixture.cluster, gcsPath, isEditMode).lastLockedBy

                gcsLockedBy should not be None
                welderLockedBy should not be None
                gcsLockedBy shouldBe welderLockedBy
              }
            }
          }
        }
      }
    }

    "open notebook in playground mode should work" in { clusterFixture =>
      val sampleNotebook = ResourceFile("bucket-tests/gcsFile.ipynb")
      val isEditMode = false

      withResourceFileInBucket(clusterFixture.billingProject, sampleNotebook, "text/plain") { gcsPath =>
        logger.info("Initialized google storage bucket")

        withWelderInitialized(clusterFixture.cluster, gcsPath, isEditMode) { localizedFile =>

          withWebDriver { implicit driver =>

            withOpenNotebook(clusterFixture.cluster, localizedFile, 2.minutes) { notebookPage =>

              val originalRemoteContentSize: Int = getObjectSize(gcsPath.bucketName, GcsBlobName(gcsPath.objectName.value))
                .unsafeRunSync()

              val originalLocalContentSize: Int = Notebook.getNotebookItem(clusterFixture.billingProject, clusterFixture.cluster.clusterName, Welder.getLocalPath(gcsPath, isEditMode)).size

              originalRemoteContentSize shouldBe originalLocalContentSize

              notebookPage.modeExists() shouldBe true
              notebookPage.getMode() shouldBe NotebookMode.SafeMode
              notebookPage.addCodeAndExecute("1+1")

              notebookPage.saveNotebook()
              //sleep 4 minutes. We do this to ensure the assertions are true after a certain about of time
              logger.info("Waiting 4 minutes as lock takes time to be reflected in metadata")
              Thread.sleep(240000)

              val newLocalContentSize = Notebook.getNotebookItem(clusterFixture.billingProject, clusterFixture.cluster.clusterName, Welder.getLocalPath(gcsPath, isEditMode)).size

              eventually(timeout(Span(5, Seconds))) {
                val newRemoteContentSize = getObjectSize(gcsPath.bucketName, GcsBlobName(gcsPath.objectName.value))
                  .unsafeRunSync()

                newLocalContentSize should be > newRemoteContentSize
                originalRemoteContentSize shouldBe newRemoteContentSize
              }

              //some selectors are omitted to simplify the test with the assumption that if the majority are hidden, they all are
              val uiElementIds: List[String]  = List("save-notbook", "new_notebook", "open_notebook", "copy_notebook", "save_notebook_as", "save_checkpoint", "restore_checkpoint", "notification_notebook")
              val areElementsHidden: Boolean = notebookPage.areElementsHidden(uiElementIds)

              areElementsHidden shouldBe true

              val gcsLockedBy: Option[String] = getLockedBy(gcsPath.bucketName, GcsBlobName(gcsPath.objectName.value)).unsafeRunSync()
              val welderLockedBy: Option[String] = Welder.getMetadata(clusterFixture.cluster, gcsPath, isEditMode).lastLockedBy

              gcsLockedBy shouldBe None
              welderLockedBy shouldBe None
            }
          }
        }
      }
    }

  }
}
