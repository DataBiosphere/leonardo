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
  override val debug = true
  mockedCluster = mockCluster("gpalloc-dev-master-3crdvwj", "automation-test-aap0v31gz")

  "Leonardo welder and notebooks" - {

//    "Welder should be up" in { clusterFixture =>
//      println("printing cluster Fixture")
//      println(clusterFixture)
//      val resp: HttpResponse = Welder.getWelderStatus(clusterFixture.cluster)
//      resp.status.isSuccess() shouldBe true
//    }

    "storageLinks and localize should work" in { clusterFixture =>
        val sampleNotebook = ResourceFile("bucket-tests/gcsFile.ipynb")
      //gpalloc-dev-master-3qqssch/automation-test-a3gbuiq6z
//      val mockedCluster = mockCluster("gpalloc-dev-master-3crdvwj", "automation-test-a3ccevftz")
        withResourceFileInBucket(clusterFixture.cluster.googleProject, sampleNotebook, "text/plain") { googleCloudDir =>

          withWelderInitialized(clusterFixture.cluster, googleCloudDir, true) { localizedPath =>
            withWebDriver { implicit driver =>
            println("====================================================")
            println("in welder initialized callback")
            println(localizedPath)
            withOpenNotebook(clusterFixture.cluster, localizedPath, 10.minutes) { notebookPage =>
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
