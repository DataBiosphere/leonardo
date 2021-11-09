/**
 *  1. Check if singleCellExp, DESeq2 etc %in% installed.packages()
 *  2. BiocManager::install() for
 *  3. BiocManager::version() == 3.6.1
 *  4. BiocManager::install('rsbml')
 *  5. BiocManager::install('RCurl') ## tests libcurl
 *  6. "XML" ##libxml
 *  7. "graphviz" RGraphviz
 *  8. "BiocSklearn"
 *  9. "rhhdf5"
 *  10. openbabel - ChemmineOB
 *  11. gsl - DirichletMultinomial
 *  12. gtk - RGtk2
 *  13. magick++ EBImage
 *  14. protobuf- protolite
 *  15. databse stuff - RMySQL
 *  16. jags - rjags
 */
package org.broadinstitute.dsde.workbench.leonardo.notebooks

import cats.effect.unsafe.implicits.global
import org.broadinstitute.dsde.workbench.auth.AuthToken
import org.broadinstitute.dsde.workbench.leonardo.{LeonardoConfig, RuntimeFixtureSpec}
import org.scalatest.DoNotDiscover

/**
 * This spec verifies notebook functionality specifically around the R-Bioconductor kernel.
 */
@DoNotDiscover
class NotebookBioconductorKernelSpec extends RuntimeFixtureSpec with NotebookTestUtils {
  implicit def ronToken: AuthToken = ronAuthToken.unsafeRunSync()

  override val toolDockerImage: Option[String] = Some(LeonardoConfig.Leonardo.bioconductorImageUrl)
  "NotebookBioconductorKernelSpec" - {
    "should have Java available" in { runtimeFixture =>
      withWebDriver { implicit driver =>
        withNewNotebook(runtimeFixture.runtime, RKernel) { notebookPage =>
          val javaOutput = notebookPage.executeCell("""system('java --version', intern = TRUE)""")
          javaOutput shouldBe defined
          javaOutput.get should include("OpenJDK Runtime Environment")
          javaOutput.get should not include ("not found")
        }
      }
    }
  }
}
