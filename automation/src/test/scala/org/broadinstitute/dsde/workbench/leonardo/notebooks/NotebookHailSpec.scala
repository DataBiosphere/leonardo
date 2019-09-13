package org.broadinstitute.dsde.workbench.leonardo.notebooks

import java.nio.file.Files

import org.broadinstitute.dsde.workbench.ResourceFile
import org.broadinstitute.dsde.workbench.leonardo.{ClusterFixtureSpec, LeonardoConfig}
import org.broadinstitute.dsde.workbench.service.util.Tags
import org.scalatest.DoNotDiscover

import scala.collection.JavaConverters._
import scala.concurrent.duration.DurationInt

/**
  * This spec verifies Hail and Spark functionality.
  */
@DoNotDiscover
class NotebookHailSpec extends ClusterFixtureSpec with NotebookTestUtils {

  // Should match the HAILHASH env var in the Jupyter Dockerfile
  val expectedHailVersion = "devel-9d6bf0096349"
  val hailTutorialUploadFile = ResourceFile(s"diff-tests/hail-tutorial.ipynb")
  override val jupyterDockerImage: Option[String] = Some(LeonardoConfig.Leonardo.hailImageUrl)


  "NotebookHailSpec" - {

    // See https://hail.is/docs/stable/tutorials-landing.html
    // Note this is for the stable Hail version (0.1). The tutorial script has changed in Hail 0.2.
    // TODO: ignored until this can be made to work with Hail 0.2s
    "should run the Hail tutorial" ignore { clusterFixture =>
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
              |hl.init()
            """.stripMargin

          val importHailOutput =
            s"""Welcome to
               |     __  __     <>__
               |    / /_/ /__  __/ /
               |   / __  / _ `/ / /
               |  /_/ /_/\\_,_/_/_/   version $expectedHailVersion""".stripMargin

          notebookPage.executeCell(importHail).get should include (importHailOutput)
        }
      }
    }

    val sparkJobToSucceed =
      """import random
        |import hail as hl
        |sc = hl.spark_context()
        |NUM_SAMPLES=20
        |def inside(p):
        |    x, y = random.random(), random.random()
        |    return x*x + y*y < 1
        |
        |count = sc.parallelize(range(0, NUM_SAMPLES)) \
        |             .filter(inside).count()
        |print("Pi is roughly %f" % (4.0 * count / NUM_SAMPLES))""".stripMargin


    s"should be able to run a Spark job with a ${Python3.string} kernel" in { clusterFixture =>
      withWebDriver { implicit driver =>
        withNewNotebook(clusterFixture.cluster, Python3) { notebookPage =>
          val cellResult = notebookPage.executeCell(sparkJobToSucceed).get
          cellResult should include("Pi is roughly ")
          cellResult.toLowerCase should not include "error"
        }
      }
    }
  }

}
