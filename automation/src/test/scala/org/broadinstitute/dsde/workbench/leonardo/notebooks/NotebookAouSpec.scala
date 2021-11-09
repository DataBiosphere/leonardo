package org.broadinstitute.dsde.workbench.leonardo.notebooks

import org.broadinstitute.dsde.workbench.leonardo.{ContainerRegistry, LeonardoConfig, RuntimeFixtureSpec}
import org.scalatest.DoNotDiscover

/**
 * This spec verifies expected functionality of the Terra AoU Jupyter image.
 */
@DoNotDiscover
class NotebookAouSpec extends RuntimeFixtureSpec with NotebookTestUtils {
  override val toolDockerImage: Option[String] = Some(LeonardoConfig.Leonardo.aouImageUrl)
  override val welderRegistry: Option[ContainerRegistry] = Some(ContainerRegistry.DockerHub)

  "NotebookAoUSpec" - {
    "should have wondershaper installed" in { runtimeFixture =>
      withWebDriver { implicit driver =>
        withNewNotebook(runtimeFixture.runtime, Python3) { notebookPage =>
          val result = notebookPage.executeCell("!command -v wondershaper")
          result shouldBe defined
          result.get should include("/usr/local/sbin/wondershaper")
        }
      }
    }

    "should have Pandas automatically installed" in { runtimeFixture =>
      withWebDriver { implicit driver =>
        withNewNotebook(runtimeFixture.runtime, Python3) { notebookPage =>
          notebookPage.executeCell("import pandas") should (be(None) or be(
            Some("Matplotlib is building the font cache; this may take a moment.")
          ))
        }
      }
    }

    "should have bigrquery automatically installed" in { runtimeFixture =>
      withWebDriver { implicit driver =>
        withNewNotebook(runtimeFixture.runtime, RKernel) { notebookPage =>
          notebookPage.executeCell(""""bigrquery" %in% installed.packages()""") shouldBe Some("TRUE")
        }
      }
    }
  }
}
