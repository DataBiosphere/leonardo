package org.broadinstitute.dsde.workbench.leonardo.notebooks

import java.nio.file.Files

import org.broadinstitute.dsde.workbench.ResourceFile
import org.broadinstitute.dsde.workbench.leonardo.ClusterFixtureSpec
import org.broadinstitute.dsde.workbench.service.util.Tags

import scala.collection.JavaConverters._
import scala.concurrent.duration.DurationInt
import scala.language.postfixOps

class NotebookHailSpec extends ClusterFixtureSpec with NotebookTestUtils {

  "Leonardo notebooks" - {
    // Should match the HAILHASH env var in the Jupyter Dockerfile
    val expectedHailVersion = "0.2.11-bb41118d9587"
    val hailTutorialUploadFile = ResourceFile(s"diff-tests/hail-tutorial.ipynb")

    // See https://hail.is/docs/stable/tutorials-landing.html
    // Note this is for the stable Hail version (0.1). The tutorial script has changed in Hail 0.2.
    "should run the Hail tutorial" in { clusterFixture =>
      val downloadDir = createDownloadDirectory()
      withWebDriver(downloadDir) { implicit driver =>
        uploadDownloadTest(clusterFixture.cluster, hailTutorialUploadFile, 15.minutes, downloadDir) { (uploadFile, downloadFile) =>
          // There are many differences including timestamps, so we can't really compare uploadFile
          // and downloadFile correctly. For now just verify the absence of ClassCastExceptions, which is the
          // issue reported in https://github.com/DataBiosphere/leonardo/issues/222.
          val downloadFileContents: String = Files.readAllLines(downloadFile.toPath).asScala.mkString
          downloadFileContents should not include "ClassCastException"
        }
      }
    }

    "should install the right Hail version" taggedAs Tags.SmokeTest in { clusterFixture =>
      withWebDriver { implicit driver =>
        withNewNotebook(clusterFixture.cluster, PySpark3) { notebookPage =>
          val importHail =
            """import hail as hl
              |hl.init(sc)
            """.stripMargin

          val importHailOutput =
            s"""Welcome to
               |     __  __     <>__
               |    / /_/ /__  __/ /
               |   / __  / _ `/ / /
               |  /_/ /_/\_,_/_/_/   version $expectedHailVersion""".stripMargin

          notebookPage.executeCell(importHail).get should include (importHailOutput)
        }
      }
    }

  }

}
