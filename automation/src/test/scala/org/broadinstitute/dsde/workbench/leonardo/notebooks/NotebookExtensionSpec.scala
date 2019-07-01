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
  mockedCluster = mockCluster("billing-leo-env-dev-0","c1")

  "Leonardo welder and notebooks" - {

//    "Welder should be up" in { clusterFixture =>
//      println("printing cluster Fixture")
//      println(clusterFixture)
//      val resp: HttpResponse = Welder.getWelderStatus(clusterFixture.cluster)
//      resp.status.isSuccess() shouldBe true
//    }

    "storageLinks and localize should work" in { clusterFixture =>
        val sampleNotebook = ResourceFile("bucket-tests/gcsFile.ipynb")
        withResourceFileInBucket(clusterFixture.billingProject, sampleNotebook, "text/plain") { googleCloudDir =>
          println("====================================================")

          withWelderInitialized(clusterFixture.cluster, googleCloudDir, true) { localizedFile =>
            withWebDriver { implicit driver =>
            println("====================================================")
            println("in welder initialized callback")
            println(localizedFile)
              println("sleeping for a minute")
              Thread.sleep(60000)
            withOpenNotebook(clusterFixture.cluster, localizedFile, 10.minutes) { notebookPage =>
              logger.info("notebook is open")
//              notebookPage.executeCell("! jupyter nbextension list ")
              Thread.sleep(10000000)
//                notebookPage.close()
            }
          }
        }
        }
      }

  }
}
