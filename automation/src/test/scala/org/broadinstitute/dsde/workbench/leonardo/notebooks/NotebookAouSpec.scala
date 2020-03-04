package org.broadinstitute.dsde.workbench.leonardo.notebooks

import org.broadinstitute.dsde.workbench.leonardo.{ClusterFixtureSpec, LeonardoConfig}
import org.scalatest.DoNotDiscover

/**
 * This spec verifies expected functionality of the Terra AoU Jupyter image.
 */
@DoNotDiscover
class NotebookAouSpec extends ClusterFixtureSpec with NotebookTestUtils {
  override val toolDockerImage: Option[String] = Some(LeonardoConfig.Leonardo.aouImageUrl)
  "NotebookAoUSpec" - {

    "should have wondershaper installed" in { clusterFixture =>
      withWebDriver { implicit driver =>
        withNewNotebook(clusterFixture.cluster, Python3) { notebookPage =>
          notebookPage.executeCell("!command -v wondershaper") shouldBe include("/usr/bin/wondershaper")
        }
      }
    }

    "should have Pandas automatically installed" in { clusterFixture =>
      withWebDriver { implicit driver =>
        withNewNotebook(clusterFixture.cluster, Python3) { notebookPage =>
          notebookPage.executeCell("import pandas") shouldBe None
        }
      }
    }

    "should have bigrquery automatically installed" in { clusterFixture =>
      withWebDriver { implicit driver =>
        withNewNotebook(clusterFixture.cluster, RKernel) { notebookPage =>
          notebookPage.executeCell(""""bigrquery" %in% installed.packages()""") shouldBe Some("TRUE")
        }
      }
    }


  }
}
