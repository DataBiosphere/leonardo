package org.broadinstitute.dsde.workbench.leonardo.notebooks

import org.broadinstitute.dsde.workbench.leonardo.{LeonardoConfig, RuntimeFixtureSpec}
import org.scalatest.DoNotDiscover

/**
 * This spec verifies expected functionality of the Terra GATK+Samtools image.
 */
@DoNotDiscover
class NotebookGATKSpec extends RuntimeFixtureSpec with NotebookTestUtils {

  override val toolDockerImage: Option[String] = Some(LeonardoConfig.Leonardo.gatkImageUrl)
  "NotebookGATKSpec" - {

    "should install Python packages, R, GATK, Samtools, and Java" in { runtimeFixture =>
      withWebDriver { implicit driver =>
        withNewNotebook(runtimeFixture.runtime, Python3) { notebookPage =>
          val pythonOutput = notebookPage.executeCell("""! pip3 show tensorflow_cpu""")
          pythonOutput shouldBe defined
          pythonOutput.get should include("Name: tensorflow-cpu")

          val rOutput = notebookPage.executeCell("""! R --version""")
          rOutput shouldBe defined
          rOutput.get should include("R version")
          rOutput.get should not include ("not found")

          val gatkOutput = notebookPage.executeCell("""! gatk --version""")
          gatkOutput shouldBe defined
          gatkOutput.get should include("Using GATK jar")
          gatkOutput.get should not include ("not found")

          val samtoolsOutput = notebookPage.executeCell("""! samtools --version""")
          samtoolsOutput shouldBe defined
          samtoolsOutput.get should include("Using htslib")
          samtoolsOutput.get should not include ("not found")

          val javaOutput = notebookPage.executeCell("""! java -version""")
          javaOutput shouldBe defined
          javaOutput.get should include("openjdk version \"1.8.0_")
          javaOutput.get should include("OpenJDK Runtime Environment")
          javaOutput.get should not include ("not found")
        }
      }
    }
  }
}
