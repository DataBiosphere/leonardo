package org.broadinstitute.dsde.workbench.leonardo.notebooks

import org.broadinstitute.dsde.workbench.ResourceFile
import org.broadinstitute.dsde.workbench.leonardo.{ClusterFixtureSpec, LeonardoConfig}
import org.broadinstitute.dsde.workbench.service.util.Tags
import org.scalatest.DoNotDiscover

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
    "should install the right Hail version" taggedAs Tags.SmokeTest in { clusterFixture =>
      withWebDriver { implicit driver =>
        withNewNotebook(clusterFixture.cluster, Python3) { notebookPage =>
          // Verify we have the right hail version
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

          notebookPage.executeCell(importHail, cellNumberOpt = Some(1)).get should include(importHailOutput)

          // Run the Hail tutorial and verify
          // https://hail.is/docs/0.2/tutorials-landing.html
          val tutorialToRun =
            """
              |hl.utils.get_movie_lens('data/')
              |users = hl.read_table('data/users.ht')
              |users.aggregate(hl.agg.count())""".stripMargin
          val tutorialCellResult = notebookPage.executeCellWithCellOutput(tutorialToRun, cellNumberOpt = Some(2)).get
          tutorialCellResult.output.get.toInt shouldBe (943)

          // Verify spark job works
          val sparkJobToSucceed =
            """import random
              |hl.balding_nichols_model(3, 1000, 1000)._force_count_rows()""".stripMargin
          val sparkJobToSucceedcellResult =
            notebookPage.executeCellWithCellOutput(sparkJobToSucceed, cellNumberOpt = Some(3)).get
          sparkJobToSucceedcellResult.output.get.toInt shouldBe (1000)
        }
      }
    }
  }
}
