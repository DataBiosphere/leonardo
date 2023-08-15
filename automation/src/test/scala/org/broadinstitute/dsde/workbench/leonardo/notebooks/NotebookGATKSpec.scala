package org.broadinstitute.dsde.workbench.leonardo.notebooks

import cats.effect.unsafe.implicits.global
import org.broadinstitute.dsde.workbench.auth.AuthToken
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
      implicit def ronToken: AuthToken = ronAuthToken.unsafeRunSync()
      withWebDriver { implicit driver =>
        withNewNotebook(runtimeFixture.runtime, Python3) { notebookPage =>
          val rOutput = notebookPage.executeCell("""! R --version""")
          rOutput shouldBe defined
          rOutput.get should include("R version")
          rOutput.get should not include "not found"

          val gatkOutput = notebookPage.executeCell("""! gatk --version""")
          gatkOutput shouldBe defined
          gatkOutput.get should include("Using GATK jar")
          gatkOutput.get should not include "not found"

          val samtoolsOutput = notebookPage.executeCell("""! samtools --version""")
          samtoolsOutput shouldBe defined
          samtoolsOutput.get should include("Using htslib")
          samtoolsOutput.get should not include "not found"
        }
      }
    }
  }
}
