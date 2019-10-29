package org.broadinstitute.dsde.workbench.leonardo.notebooks

import org.broadinstitute.dsde.workbench.leonardo.{ClusterFixtureSpec, LeonardoConfig}
import org.scalatest.DoNotDiscover

/**
 * This spec verifies expected functionality of the Terra GATK+Samtools image.
 */
@DoNotDiscover
class NotebookGATKSpec extends ClusterFixtureSpec with NotebookTestUtils {

  override val jupyterDockerImage: Option[String] = Some(LeonardoConfig.Leonardo.gatkImageUrl)
  "NotebookGATKSpec" - {

    "should install Python packages, R, GATK, and Samtools" in { clusterFixture =>
      withWebDriver { implicit driver =>
        withNewNotebook(clusterFixture.cluster, Python3) { notebookPage =>
          val pythonOutput = notebookPage.executeCell("""! pip3 show tensorflow""")
          pythonOutput shouldBe 'defined
          pythonOutput.get should include("Name: tensorflow")

          val rOutput = notebookPage.executeCell("""! R --version""")
          rOutput shouldBe 'defined
          rOutput.get should include("R version")
          rOutput.get should not include ("not found")

          val gatkOutput = notebookPage.executeCell("""! gatk --version""")
          gatkOutput shouldBe 'defined
          gatkOutput.get should include("Using GATK jar")
          gatkOutput.get should not include ("not found")

          val samtoolsOutput = notebookPage.executeCell("""! samtools --version""")
          samtoolsOutput shouldBe 'defined
          samtoolsOutput.get should include("Using htslib")
          samtoolsOutput.get should not include ("not found")
        }
      }
    }
  }
}
