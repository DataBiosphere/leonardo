package org.broadinstitute.dsde.workbench.leonardo.notebooks


import java.net.URL
import java.time.Instant
import java.util.UUID

import akka.http.scaladsl.model.HttpResponse
import org.broadinstitute.dsde.workbench.ResourceFile
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

//  debug = true
  mockedCluster = mockCluster("gpalloc-dev-master-3crdvwj","automation-test-apb9t9xsz")
//
  "Leonardo welder and notebooks" - {

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

            withOpenNotebook(clusterFixture.cluster, localizedFile, 200.minutes) { notebookPage =>
              logger.info("notebook is open")

              Thread.sleep(100000000)
              notebookPage.modeExists() shouldBe true
              notebookPage.getMode() shouldBe Notebook.EditMode
              //              notebookPage.executeCell("print('test')")
              //              notebookPage.clickSave()

              //              val content: ContentItem = Notebook.getContentItem(clusterFixture.billingProject, clusterFixture.cluster.clusterName, localizedFile.getPath)

              //              logger.info("Printing content from notebook: " + content.content)
              //TODO make this a real check
              //              content shouldBe content
              //                notebookPage.close()
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
//        withWelderInitialized(clusterFixture.cluster, googleCloudDir, true) { localizedFile =>
//          withWebDriver { implicit driver =>
//
//            withOpenNotebook(clusterFixture.cluster, localizedFile, 2.minutes) { notebookPage =>
//              logger.info("notebook is open")
//              notebookPage.modeExists() shouldBe true
//              notebookPage.getMode() shouldBe Notebook.SafeMode
//              notebookPage.executeCell("print('test')")
//              notebookPage.clickSave()
////              Thread.sleep(100000000)
//              //                notebookPage.close()
//            }
//          }
//        }
//      }
//    }

  }
}
