package org.broadinstitute.dsde.workbench.leonardo.notebooks


import akka.http.scaladsl.model.HttpResponse
import org.broadinstitute.dsde.workbench.ResourceFile
import org.broadinstitute.dsde.workbench.leonardo._

import scala.concurrent._
import scala.concurrent.duration._

import scala.language.postfixOps


class NotebookExtensionSpec extends ClusterFixtureSpec with NotebookTestUtils {
  override def enableWelder: Boolean = true

  "Leonardo welder and notebooks" - {

//    "Welder should be up" in { clusterFixture =>
//      println("printing cluster Fixture")
//      println(clusterFixture)
//      val resp: HttpResponse = Welder.getWelderStatus(clusterFixture.cluster)
//      resp.status.isSuccess() shouldBe true
//    }

    "storageLinks and localize should work" in { clusterFixture =>
      withWebDriver { implicit driver =>
        val sampleNotebook = ResourceFile("bucket-tests/gcsFile.ipynb")
        withResourceFileInBucket(clusterFixture.billingProject, sampleNotebook, "text/plain") { googleCloudDir =>


          println("====================================================")
          println("in welder test code")

          withWelderInitialized(clusterFixture.cluster, googleCloudDir, true) { localizedFile =>
            println("====================================================")
            println("in welder initialized callback")
            println(localizedFile)
            withOpenNotebook(clusterFixture.cluster, localizedFile, 10.minutes) { notebookPage =>
                notebookPage.executeCell("! jupyter nbextension list ")
//                notebookPage.close()
            }
          }
          //                //withOpenNotebook(cluster, localizedPath, 10.minutes) {
        }
        }
      }

  }
}
