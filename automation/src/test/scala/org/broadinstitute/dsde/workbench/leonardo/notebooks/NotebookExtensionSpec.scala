package org.broadinstitute.dsde.workbench.leonardo.notebooks


import java.net.URL
import java.time.Instant
import java.util.UUID

import akka.http.scaladsl.model.HttpResponse
import org.broadinstitute.dsde.workbench.ResourceFile
import org.broadinstitute.dsde.workbench.google2.GcsBlobName
import org.broadinstitute.dsde.workbench.leonardo.ClusterStatus.ClusterStatus
import org.broadinstitute.dsde.workbench.leonardo.StringValueClass.LabelMap
import org.broadinstitute.dsde.workbench.leonardo._
import org.broadinstitute.dsde.workbench.model.WorkbenchEmail
import org.broadinstitute.dsde.workbench.model.google.{GcsBucketName, GcsPath, GoogleProject}

import scala.concurrent._
import scala.concurrent.duration._
import scala.language.postfixOps


class NotebookExtensionSpec extends ClusterFixtureSpec with NotebookTestUtils {
  override def enableWelder: Boolean = true

  debug = true
  mockedCluster = mockCluster("gpalloc-dev-master-2b9yymo","automation-test-a3ggrwl1z")
//
  "Leonardo welder and jupyter extensions" - {

//    "Welder should be up" in { clusterFixture =>
//      println("printing cluster Fixture")
//      println(clusterFixture)
//      val resp: HttpResponse = Welder.getWelderStatus(clusterFixture.cluster)
//      resp.status.isSuccess() shouldBe true
//    }

    "open notebook in edit mode should work" in { clusterFixture =>
        val sampleNotebook = ResourceFile("bucket-tests/gcsFile.ipynb")
        withResourceFileInBucket(clusterFixture.billingProject, sampleNotebook, "text/plain") { googleCloudDir =>
          logger.info("Initialized google storage bucket")

          withWelderInitialized(clusterFixture.cluster, googleCloudDir, true) { localizedFile =>
            withWebDriver { implicit driver =>

            withOpenNotebook(clusterFixture.cluster, localizedFile, 200.minutes) { notebookPage => //TODO fix timeout
              logger.info("notebook is open")

              notebookPage.modeExists() shouldBe true
              notebookPage.getMode() shouldBe Notebook.EditMode

              notebookPage.addCodeAndExecute("1+1")
              logger.info("clicking save")
              notebookPage.clickSave()

              logger.info("Waiting 30 seconds to ensure notebook is saved to remote.")
              Thread.sleep(5000)

//              val localContent: String = getNotebookFileAsString(ResourceFile("bucket-tests/gcsFile.ipynb")) //we reinitialize the resource file because we have written to it
//                                          .split("\n").map(_.trim.filter(_ >= " ")).mkString

              //we must use any because we need to comparing a common predecessor of the jupyter server"s file with a bunch of irrelevant metadata with google"s raw content return
              val localContentSize: Long = Notebook.getNotebookItem(clusterFixture.billingProject, clusterFixture.cluster.clusterName, Welder.getLocalPath(googleCloudDir, true)).size.toLong

              val remoteContentSize: Long = getObjectAsFile(googleCloudDir.bucketName, GcsBlobName(googleCloudDir.objectName.value)).length()

              logger.info("===========================")
              logger.info("Printing content from local notebook: " + localContentSize)
              logger.info("Printing content from remote notebook: " + remoteContentSize)

              remoteContentSize shouldBe localContentSize

//                            Thread.sleep(30000)

              val lockedBy: Option[String] = getLockedBy(googleCloudDir.bucketName, GcsBlobName(googleCloudDir.objectName.value)).unsafeRunSync()
              logger.info("notebook is locked by: " + lockedBy)

              Thread.sleep(1000000000)
              notebookPage.close()
            }
          }
        }
        }
      }

//    "open notebook in playground mode should work" in { clusterFixture =>
//      val sampleNotebook = ResourceFile("bucket-tests/gcsFile.ipynb")
//      withResourceFileInBucket(clusterFixture.billingProject, sampleNotebook, "text/plain") { googleCloudDir =>
//        logger.info("Initialized google storage bucket")
//
//        withWelderInitialized(clusterFixture.cluster, googleCloudDir, false) { localizedFile =>
//          withWebDriver { implicit driver =>
//
//            withOpenNotebook(clusterFixture.cluster, localizedFile, 2.minutes) { notebookPage =>
//              logger.info("notebook is open")
//              notebookPage.modeExists() shouldBe true
//              notebookPage.getMode() shouldBe Notebook.SafeMode
//              notebookPage.addCodeAndExecute("1+1")
//               val localContentSize: Long = Notebook.getNotebookItem(clusterFixture.billingProject, clusterFixture.cluster.clusterName, Welder.getLocalPath(googleCloudDir, true)).size.toLong
//
//              val remoteContentSize: Long = getObjectAsFile(googleCloudDir.bucketName, GcsBlobName(googleCloudDir.objectName.value)).length()
//
//              notebookPage.clickSave()
//
//              //some selectors are omitted to simplfiy the test with the assumption that if the majority are hidden, they all are
//              val uiElementIds: List[String]  = List("#save-notbook", "#new_notebook", "#open_notebook", "#copy_notebook", "#save_notebook_as", "#save_checkpoint", "#restore_checkpoint", "#notification_notebook", "#file_menu > li.divider:eq(0)", "#file_menu > li.divider:eq(2)")
//      //TODO verify UI elements gone
//              val areElementsHidden: Boolean = notebookPage.areElementsHidden(uiElementIds)
//
//              localContentSize should be > remoteContentSize
//              areElementsHidden shouldBe true
//
//              Thread.sleep(100000000)
//              notebookPage.close()
//            }
//          }
//        }
//      }
//    }

  }
}
